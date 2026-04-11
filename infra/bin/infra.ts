#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { InfraStack } from '../lib/infra-stack';
import { ENVIRONMENTS_CONFIG, Stage } from './constants';
import { MasterPipeline } from '../lib/pipeline-stack';

const app = new cdk.App();

new InfraStack(app, 'DagsterInfraStack-production', {
  stage: Stage.production
});

// Create the pipeline stack that will manage deployments
new MasterPipeline(app, 'DagsterPipelineStack', {
  env: {
    region: 'us-east-1',
    account: '612658240832' // staging
  }
});