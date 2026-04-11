# Dagster AWS Deployment Guide

This guide explains how to deploy your Dagster application to AWS EC2 instances using CodeBuild and CodeDeploy with UV dependency management.

## Files Overview

- **`buildspec.yml`** - CodeBuild configuration for building and packaging the application
- **`appspec.yml`** - CodeDeploy configuration for deployment to EC2 instances
- **`scripts/`** - Deployment scripts for different phases of the deployment process

## Prerequisites

1. **AWS Account** with appropriate permissions for CodeBuild, CodeDeploy, and EC2
2. **EC2 Instances** running Amazon Linux 2 or Ubuntu with CodeDeploy agent installed
3. **IAM Roles** configured for CodeBuild and CodeDeploy
4. **S3 Bucket** for storing build artifacts

## Setup Instructions

### 1. Create CodeBuild Project

1. Go to AWS CodeBuild console
2. Create a new build project
3. Use the `buildspec.yml` file from this repository
4. Configure source repository (GitHub, CodeCommit, etc.)
5. Set environment to use Ubuntu standard:2.0 or Amazon Linux 2
6. Configure IAM role with permissions for:
   - S3 access
   - CloudWatch Logs
   - CodeDeploy

### 2. Create CodeDeploy Application

1. Go to AWS CodeDeploy console
2. Create a new application
3. Choose "EC2/On-premises" as the compute platform
4. Create a deployment group with your EC2 instances
5. Configure the service role with appropriate permissions

### 3. Configure EC2 Instances

Ensure your EC2 instances have:

```bash
# Install CodeDeploy agent
sudo yum update -y
sudo yum install -y ruby wget
cd /home/ec2-user
wget https://aws-codedeploy-${AWS_REGION}.s3.${AWS_REGION}.amazonaws.com/latest/install
chmod +x ./install
sudo ./install auto
sudo service codedeploy-agent start
sudo chkconfig codedeploy-agent on

# Install required packages
sudo yum install -y curl systemd
```

### 4. IAM Role Configuration

Create an IAM role for your EC2 instances with the following policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:Get*",
                "s3:List*"
            ],
            "Resource": [
                "arn:aws:s3:::your-artifact-bucket/*"
            ]
        }
    ]
}
```

## Deployment Process

### Build Phase (CodeBuild)

1. **Install Phase**: Installs UV package manager
2. **Pre-build Phase**: Sets up UV environment and syncs dependencies
3. **Build Phase**: Builds the application using UV
4. **Post-build Phase**: Creates deployment package with all necessary files

### Deployment Phase (CodeDeploy)

1. **BeforeInstall**: Stops existing services and creates backups
2. **AfterInstall**: Sets up UV environment and creates systemd services
3. **ApplicationStart**: Starts Dagster webserver and daemon services
4. **ValidateService**: Verifies services are running and responding

## Service Configuration

The deployment creates two systemd services:

- **`dagster-webserver`**: Runs on port 3000
- **`dagster-daemon`**: Background daemon for job scheduling

### Service Management

```bash
# Check service status
sudo systemctl status dagster-webserver
sudo systemctl status dagster-daemon

# View logs
sudo journalctl -u dagster-webserver -f
sudo journalctl -u dagster-daemon -f

# Restart services
sudo systemctl restart dagster-webserver
sudo systemctl restart dagster-daemon
```

## Monitoring and Troubleshooting

### Health Checks

- Webserver health endpoint: `http://your-ec2-ip:3000/healthz`
- Service status: `systemctl status dagster-*`
- Logs: `journalctl -u dagster-*`

### Common Issues

1. **Port conflicts**: Ensure port 3000 is available and not blocked by security groups
2. **Permission issues**: Check file ownership and permissions in `/home/ubuntu/dagster`
3. **UV installation**: Verify UV is properly installed and in PATH
4. **Service failures**: Check logs for specific error messages

## Security Considerations

1. **Security Groups**: Configure EC2 security groups to allow traffic on port 3000
2. **IAM Roles**: Use least-privilege principle for IAM roles
3. **Network Access**: Consider using Application Load Balancer for external access
4. **Secrets**: Use AWS Secrets Manager or Parameter Store for sensitive configuration

## Customization

### Environment Variables

Add environment-specific configuration in the deployment scripts:

```bash
# In after_install.sh
export DAGSTER_HOME=/home/ubuntu/dagster
export DAGSTER_CONFIG=/home/ubuntu/dagster/config
```

### Additional Dependencies

Modify the `buildspec.yml` to include additional build steps or dependencies as needed.

## Rollback

The deployment automatically creates backups in `/home/ubuntu/dagster.backup.*`. To rollback:

1. Stop current services
2. Restore from backup
3. Restart services

```bash
sudo systemctl stop dagster-webserver dagster-daemon
sudo mv /home/ubuntu/dagster /home/ubuntu/dagster.failed
sudo mv /home/ubuntu/dagster.backup.* /home/ubuntu/dagster
sudo systemctl start dagster-webserver dagster-daemon
```

## Support

For issues or questions:
1. Check CodeBuild and CodeDeploy logs in AWS console
2. Review EC2 instance logs and systemd service logs
3. Verify IAM permissions and security group configurations
