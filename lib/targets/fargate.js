import { EC2Client, DescribeVpcsCommand, DescribeSubnetsCommand } from "@aws-sdk/client-ec2";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import { LambdaClient, GetFunctionConfigurationCommand } from "@aws-sdk/client-lambda";
import logger from "leo-logger";

let defaultVPCId;
let defaultSubnetIds;

export default async function(target, payload, opts = {}) {
    let region = opts.region || process.env.AWS_DEFAULT_REGION;
    const match = target.lambdaName.match(/^arn:aws:lambda:(.*?):/);
    if (match) {
        region = match[1];
    }

    const ec2 = new EC2Client({ region });
    const ecs = new ECSClient({ region });
    const lambda = new LambdaClient({ region });

    if (!defaultVPCId) {
        const vpcData = await ec2.send(new DescribeVpcsCommand({
            Filters: [{ Name: "isDefault", Values: ["true"] }]
        }));
        const vpc = vpcData.Vpcs[0] || {};
        defaultVPCId = vpc.VpcId;
    }

    if (defaultVPCId && !defaultSubnetIds) {
        const subnetData = await ec2.send(new DescribeSubnetsCommand({
            Filters: [{ Name: "vpc-id", Values: [defaultVPCId] }]
        }));
        defaultSubnetIds = subnetData.Subnets.map(s => s.SubnetId);
    }

    try {
        const functionConfig = await lambda.send(new GetFunctionConfigurationCommand({
            FunctionName: target.lambdaName,
            Qualifier: target.qualifier
        }));

        if (!functionConfig.VpcConfig.SubnetIds.length) {
            functionConfig.VpcConfig.SubnetIds = defaultSubnetIds;
            target.publicIp = true;
        }

        payload.__cron.time += 60 * 1000;

        const params = {
            cluster: target.cluster || "default",
            taskDefinition: target.task || "CronTask",
            launchType: target.launchType || "FARGATE",
            overrides: {
                containerOverrides: [{
                    name: target.container || "leo-docker",
                    environment: [
                        { name: "BOT", value: target.lambdaName },
                        { name: "AWS_REGION", value: region },
                        { name: "LEO_EVENT", value: JSON.stringify(payload) },
                        { name: "AWS_LAMBDA_FUNCTION_NAME", value: target.lambdaName }
                    ]
                }]
            },
            networkConfiguration: {
                awsvpcConfiguration: {
                    assignPublicIp: target.publicIp ? "ENABLED" : "DISABLED",
                    subnets: functionConfig.VpcConfig.SubnetIds,
                    securityGroups: functionConfig.VpcConfig.SecurityGroupIds
                }
            },
            startedBy: "Leo Cron++"
        };

        logger.log(JSON.stringify(params, null, 2));

        const ecsResponse = await ecs.send(new RunTaskCommand(params));
        return ecsResponse;
    } catch (err) {
        logger.error("Error running ECS task:", err);
        throw err;
    }
};
