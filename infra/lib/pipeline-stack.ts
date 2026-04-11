import * as cdk from 'aws-cdk-lib';
import * as codepipeline from 'aws-cdk-lib/aws-codepipeline';
import * as codepipeline_actions from 'aws-cdk-lib/aws-codepipeline-actions';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as iam from 'aws-cdk-lib/aws-iam';

import { Construct } from 'constructs';
import { CODE_STAR_CONNECTION_ARN, ENVIRONMENTS_CONFIG, PIPELINE_BUCKET_ARTIFACTS, PIPELINE_BUCKET_ARTIFACTS_KEY, PIPELINE_NAME, PROJECT_NAME, Stage } from '../bin/constants';
import { Repository } from 'aws-cdk-lib/aws-ecr';
import { CodeDeployServerDeployAction } from 'aws-cdk-lib/aws-codepipeline-actions';
import { CfnDeploymentGroup, ServerApplication, ServerDeploymentConfig, ServerDeploymentGroup } from 'aws-cdk-lib/aws-codedeploy';


export class MasterPipeline extends cdk.Stack {

    constructor(scope: Construct, id: string, stackProps?: cdk.StackProps) {
        super(scope, id, stackProps);

        // Holds the artifacts produced all over the pipeline
        // Encryption should be managed by KMS
        const keyPolicy = new iam.PolicyDocument({
            statements: [
                new iam.PolicyStatement({
                    actions: [
                        'kms:*',
                    ],
                    principals: [
                        new iam.AccountRootPrincipal()
                    ],
                    resources: ['*']
                }),
                new iam.PolicyStatement({
                    actions: [
                        'kms:Encrypt',
                        'kms:Decrypt',
                        'kms:ReEncrypt*',
                        'kms:GenerateDataKey*',
                        'kms:DescribeKey'
                    ],
                    principals: Object.entries(ENVIRONMENTS_CONFIG).map(([stage, config]) => new iam.AccountPrincipal(config.account)),
                    resources: ['*']
                })
            ]
        });

        const key = new kms.Key(this, 'PipelineArtifactsKey', {
            alias: PIPELINE_BUCKET_ARTIFACTS_KEY,
            keySpec: kms.KeySpec.SYMMETRIC_DEFAULT,
            policy: keyPolicy
        });

        const artifactBucket = new s3.Bucket(this, 'MasterPipelineArtifactBucket', {
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            encryption: s3.BucketEncryption.KMS,
            encryptionKey: key,
            enforceSSL: true,
            removalPolicy: cdk.RemovalPolicy.RETAIN,
            bucketName: PIPELINE_BUCKET_ARTIFACTS
        });

        artifactBucket.addToResourcePolicy(new iam.PolicyStatement({
            sid: 'DenyUnEncryptedObjectUploads',
            effect: iam.Effect.DENY,
            principals: [ new iam.AnyPrincipal() ],
            actions: [ 's3:PutObject' ],
            resources: [
                artifactBucket.arnForObjects('*')
            ],
            conditions: {
                'StringNotEquals': {
                    's3:x-amz-server-side-encryption': 'aws:kms'
                }
            }
        }));

        artifactBucket.addToResourcePolicy(new iam.PolicyStatement({
            sid: 'DenyInsecureConnections',
            effect: iam.Effect.DENY,
            principals: [ new iam.AnyPrincipal() ],
            actions: [ 's3:*' ],
            resources: [
                artifactBucket.arnForObjects('*')
            ],
            conditions: {
                'Bool': {
                    'aws:SecureTransport':false
                }
            }
        }));

        artifactBucket.addToResourcePolicy(new iam.PolicyStatement({
            sid: 'AllowOperations',
            effect: iam.Effect.ALLOW,
            principals: Object.entries(ENVIRONMENTS_CONFIG).map(([stage, config]) => new iam.AccountPrincipal(config.account)),
            actions: [
                's3:Get*',
                's3:Put*',
                's3:ListBucket'
            ],
            resources: [
                artifactBucket.arnForObjects('*'),
                artifactBucket.bucketArn
            ]
        }));

        const pipeline = new codepipeline.Pipeline(this, 'Pipeline', {
            artifactBucket: artifactBucket,
            pipelineName: PIPELINE_NAME,
            pipelineType: codepipeline.PipelineType.V2
        });

        const sourceStage = pipeline.addStage({ stageName: 'Source' });
        const sourceOutput = new codepipeline.Artifact();

        // ===== Source Actions ===== //
        const codeSourceAction = new codepipeline_actions.CodeStarConnectionsSourceAction({
            actionName: 'Code',
            owner: 'Spot2HQ',
            repo: 'dagster',
            branch: 'main',
            output: sourceOutput,
            connectionArn: CODE_STAR_CONNECTION_ARN
        });

        sourceStage.addAction(codeSourceAction);

        // ===== Prod Stage ===== //
        const prodStage = pipeline.addStage({ stageName: 'Production' });

        prodStage.addAction(new CodeDeployServerDeployAction({ 
            actionName: 'Deploy',
            input: sourceOutput,
            deploymentGroup: ServerDeploymentGroup.fromServerDeploymentGroupAttributes(this, 'DagsterDeploymentGroup-production', {
                application: ServerApplication.fromServerApplicationArn(this, 'DagsterApplication-production', 'arn:aws:codedeploy:us-east-1:114302912952:application:dagster-production'),
                deploymentGroupName: Stage.production,
                deploymentConfig: ServerDeploymentConfig.ALL_AT_ONCE
            }),
        }));
    }
}