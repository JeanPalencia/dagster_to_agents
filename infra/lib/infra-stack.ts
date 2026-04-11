import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { ENVIRONMENTS_CONFIG, PIPELINE_BUCKET_ARTIFACTS, PIPELINE_BUCKET_ARTIFACTS_KEY, Stage } from '../bin/constants';
import { InstanceType, LaunchTemplate, MachineImage, Peer, Port, SecurityGroup, UserData } from 'aws-cdk-lib/aws-ec2';
import { Effect, ManagedPolicy, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { ApplicationProtocol, ApplicationTargetGroup, ListenerCondition, TargetType } from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import { AutoScalingGroup } from 'aws-cdk-lib/aws-autoscaling';
import { ServerApplication, ServerDeploymentConfig, ServerDeploymentGroup } from 'aws-cdk-lib/aws-codedeploy';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Schedule } from 'aws-cdk-lib/aws-applicationautoscaling';

export interface InfraStackProps extends cdk.StackProps {
    stage: Stage;
}

// InfraStack contains all the infrastructure required for the project.
// It includes a target group, an autoscaling group, and will use an exisiting load balancer.
export class InfraStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: InfraStackProps) {
        super(scope, id, {
            env: {
                account: ENVIRONMENTS_CONFIG[props.stage].account,
                region: ENVIRONMENTS_CONFIG[props.stage].region
            }
        });

        const vpc = ENVIRONMENTS_CONFIG[Stage.production].vpc(this, 'Vpc');
        const subnets = ENVIRONMENTS_CONFIG[Stage.production].privateSubnets(this, 'Subnets');
        const listener = ENVIRONMENTS_CONFIG[Stage.production].listener(this, 'Listener');

        // Create a security group for the dagster service
        const securityGroup = new SecurityGroup(this, 'DagsterSecurityGroup', {
            vpc,
            description: 'Security group for the dagster service',
            securityGroupName: `dagster-sg-${props.stage}`
        });

        // allow traffic from the load balancer to the dagster service
        securityGroup.addEgressRule(Peer.anyIpv4(), Port.allTraffic());
        securityGroup.addIngressRule(Peer.ipv4(vpc.vpcCidrBlock), Port.tcp(3000), 'Allow traffic from VPC');

        // Create a role to be used by the launch template
        const applicationRole = new Role(this, 'DagsterRole', {
            assumedBy: new ServicePrincipal('ec2.amazonaws.com'),
            roleName: `dagster-role-${props.stage}`
        });

        applicationRole.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'));

        // assign permissions to read parameters from SSM under the path /dagster/
        applicationRole.addToPrincipalPolicy(new PolicyStatement({
            actions: ['ssm:GetParameter'],
            resources: [`arn:aws:ssm:us-east-1:${this.account}:parameter/dagster/*`],
            effect: Effect.ALLOW
        }));

        // Create a new S3 bucket for the dagster assets
        const dagsterAssetsBucket = new Bucket(this, 'DagsterAssetsBucket', {
            bucketName: `dagster-assets-${props.stage}`
        });

        dagsterAssetsBucket.grantReadWrite(applicationRole);

        // Allow access to the pipeline artifacts
        applicationRole.addToPrincipalPolicy(new PolicyStatement({
            actions: ['s3:GetObject'],
            resources: [
                `arn:aws:s3:::${PIPELINE_BUCKET_ARTIFACTS}/*`
            ],
            effect: Effect.ALLOW
        }));

        applicationRole.addToPrincipalPolicy(new PolicyStatement({
            actions: [
                'kms:Decrypt',  
                'kms:GenerateDataKey'
            ],
            resources: [
                '*'
            ],
            effect: Effect.ALLOW
        }));

        // Create a Launch Template
        const launchTemplate = new LaunchTemplate(this, 'DagsterLaunchTemplate', {
            launchTemplateName: `dagster-lt-${props.stage}`,
            machineImage: MachineImage.fromSsmParameter('/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id'),
            instanceType: new InstanceType('r8g.xlarge'),
            securityGroup: securityGroup,
            role: applicationRole,
            userData: UserData.custom(
                `#!/bin/bash
                apt update
                apt install -y ruby-full wget
                cd /tmp
                curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
                unzip awscliv2.zip
                ./aws/install
                wget https://aws-codedeploy-us-east-1.s3.us-east-1.amazonaws.com/latest/install
                chmod +x ./install
                ./install auto
                `
            )
        });

        const asg = new AutoScalingGroup(this, 'MyAutoScalingGroup', {
            vpc,
            minCapacity: 1,
            maxCapacity: 1,
            launchTemplate,
            vpcSubnets: subnets,
            autoScalingGroupName: `dagster-asg-${props.stage}`,
        });

        // Scale out to 1 instance at 8:00 AM and scale in at 5:00 PM, mexico city time
        asg.scaleOnSchedule('ScaleOutSchedule', {
            schedule: Schedule.expression('0 8 * * *'),
            timeZone: 'America/Mexico_City',
            minCapacity: 1,
            maxCapacity: 1,
            desiredCapacity: 1,
        });

        asg.scaleOnSchedule('ScaleInSchedule', {
            schedule: Schedule.expression('0 21 * * *'),
            timeZone: 'America/Mexico_City',
            minCapacity: 0,
            maxCapacity: 0,
            desiredCapacity: 0,
        });

        const targetGroup = new ApplicationTargetGroup(this, 'DagsterTargetGroup', {
            vpc,
            port: 3000,
            protocol: ApplicationProtocol.HTTP,
            deregistrationDelay: cdk.Duration.seconds(0),
            targetGroupName: `dagster-tg-${props.stage}`,
            targetType: TargetType.INSTANCE
        });

        targetGroup.addTarget(asg);

        listener.addTargetGroups('DagsterTarget', {
            targetGroups: [targetGroup],
            priority: ENVIRONMENTS_CONFIG[Stage.production].listenerRulePriority,
            conditions: [
                ListenerCondition.hostHeaders(['dagster.spot2.mx'])
            ]
        });

        // Create the CodeDeploy application for the dagster service
        const codeDeployApplication = new ServerApplication(this, 'DagsterCodeDeployApplication', {
            applicationName: `dagster-${props.stage}`
        });

        // Create the CodeDeploy deployment group for the dagster service
        const codeDeployDeploymentGroup = new ServerDeploymentGroup(this, 'DagsterCodeDeployDeploymentGroup', {
            application: codeDeployApplication,
            deploymentGroupName: props.stage,
            deploymentConfig: ServerDeploymentConfig.ALL_AT_ONCE,
            autoScalingGroups: [asg]
        });
    }
}
