let aws = require("aws-sdk");
let logger = require("leo-logger")("leo-cron").sub("fargate");
let defaultVPCId;
let defaultSubnetIds;
// let defaultSecurityGroupId;
module.exports = async function(target, payload, opts) {

	let region = opts.region || process.env.AWS_DEFAULT_REGION;
	var match = target.lambdaName.match(/^arn:aws:lambda:(.*?):/)
	if (match) {
		region = match[1];
	}

	let ec2 = new aws.EC2({
		region: region
	});

	if (!defaultVPCId) {
		let vpc = (await ec2.describeVpcs({
			Filters: [{
				Name: "isDefault",
				Values: ["true"]
			}]
		}).promise()).Vpcs[0] || {};
		defaultVPCId = vpc.VpcId;
	}
	if (defaultVPCId && !defaultSubnetIds) {
		let subnets = (await ec2.describeSubnets({
			Filters: [{
				Name: "vpc-id",
				Values: [defaultVPCId]
			}]
		}).promise()).Subnets || [];
		defaultSubnetIds = subnets.map(s => s.SubnetId);
	}
	// if (defaultVPCId && !defaultSecurityGroupId){
	// 	let securityGroups = await ec2.describeSecurityGroups({
	// 		Filter:[{
	// 			Name: "vpc-id",
	// 			Values:[defaultVPCId]
	// 		}]
	// 	}).promise().SecurityGroups || [];
	// 	defaultSecurityGroupId = securityGroups.map(s=>s.GroupId);
	// }

	let ecs = new aws.ECS({
		region: region
	});
	let lambdaApi = new aws.Lambda({
		region: region
	});

	return new Promise((resolve, reject) => {
		lambdaApi.getFunctionConfiguration({
			FunctionName: target.lambdaName,
			Qualifier: target.qualifier
		}, (err, config) => {
			if (err) {
				return reject(err);
			}

			if (!config.VpcConfig.SubnetIds.length) {
				config.VpcConfig.SubnetIds = defaultSubnetIds
				target.publicIp = true;
			};

			payload.__cron.time += 60 * 1000
			var params = {
				cluster: target.cluster || "default",
				taskDefinition: target.task || "CronTask",
				launchType: target.launchType || "FARGATE",
				overrides: {
					containerOverrides: [{
						name: target.container || "leo-docker",
						environment: [{
							name: "BOT",
							value: target.lambdaName,
						}, {
							name: "AWS_REGION",
							value: region
						}, {
							name: "LEO_EVENT",
							value: JSON.stringify(payload)
						}, {
							name: "AWS_LAMBDA_FUNCTION_NAME",
							value: target.lambdaName
						}]
					}]
				},
				networkConfiguration: {
					awsvpcConfiguration: {
						assignPublicIp: target.publicIp ? "ENABLED" : "DISABLED",
						subnets: config.VpcConfig.SubnetIds,
						securityGroups: config.VpcConfig.SecurityGroupIds
					}
				},
				startedBy: "Leo Cron++"
			};
			logger.log(JSON.stringify(params, null, 2))
			ecs.runTask(params, function(err, data) {
				if (err) {
					reject(err);
				} else {
					resolve(data);
				}
			});

		});
	})
};