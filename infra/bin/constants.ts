import { IVpc, SecurityGroup, Subnet, SubnetSelection, Vpc } from "aws-cdk-lib/aws-ec2";
import { ApplicationListener, ApplicationLoadBalancer, IApplicationListener, IApplicationLoadBalancer } from "aws-cdk-lib/aws-elasticloadbalancingv2";
import { Construct } from "constructs";

export const PROJECT_NAME = 'Dagster';

export interface EnvironmentConfig {
    region: string;
    account: string;
    vpc: (scope: Construct, id: string) => IVpc;
    loadBalancer: (scope: Construct, id: string) => IApplicationLoadBalancer;
    privateSubnets: (scope: Construct, id: string) => SubnetSelection;
    listenerRulePriority: number;
    listener: (scope: Construct, id: string) => IApplicationListener;
};

export enum Stage {
    production = 'production'
};

export const ENVIRONMENTS_CONFIG: Record<string, EnvironmentConfig> = {
    [Stage.production]: {
        region: 'us-east-1',
        account: '114302912952',
        vpc: (scope: Construct, id: string) => Vpc.fromLookup(scope, id, {
            vpcId: 'vpc-01a57412d423980e0'
        }),
        loadBalancer: (scope: Construct, id: string) => ApplicationLoadBalancer.fromApplicationLoadBalancerAttributes(scope, id, {
            loadBalancerArn: 'arn:aws:elasticloadbalancing:us-east-1:114302912952:loadbalancer/app/geospot-alb-production/ba6536553ef5de9d',
            securityGroupId: 'sg-02cff8eb6ea342b32'
        }),
        privateSubnets: (scope: Construct, id: string) => ({
            subnets: [
                Subnet.fromSubnetId(scope, `${id}-private-subnet-1`, 'subnet-0d799d0d0453e9bff'),
                Subnet.fromSubnetId(scope, `${id}-private-subnet-2`, 'subnet-06ef0ceb343f6ef21'),
                Subnet.fromSubnetId(scope, `${id}-private-subnet-3`, 'subnet-0c5ffbec9f9539b13'),
            ]
        }),
        listener: (scope: Construct, id: string) => ApplicationListener.fromApplicationListenerAttributes(scope, id, {
            listenerArn: 'arn:aws:elasticloadbalancing:us-east-1:114302912952:listener/app/geospot-alb-production/ba6536553ef5de9d/0a9442d7ac1db985',
            securityGroup: SecurityGroup.fromSecurityGroupId(scope, `${id}-security-group`, 'sg-02cff8eb6ea342b32')
        }),
        listenerRulePriority: 40
    }
};

export const PIPELINE_NAME = `${PROJECT_NAME.toLocaleLowerCase()}-pipeline`;
export const PIPELINE_BUCKET_ARTIFACTS = `${PROJECT_NAME.toLowerCase()}-pipelineartifacts`;
export const PIPELINE_BUCKET_ARTIFACTS_KEY = `${PIPELINE_NAME}-key`;
export const CODE_STAR_CONNECTION_ARN = 'arn:aws:codestar-connections:us-east-1:612658240832:connection/3b932edb-f2f9-4f3a-8ac4-7c96529dfc9a';