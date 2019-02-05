let aws = require("aws-sdk");
let logger = require("leo-logger")("leo-cron").sub("lambda");
let lambdaWarmupOffset = process.env.lambda_warmup_offset != undefined ? parseInt(process.env.lambda_warmup_offset) : 0;
logger.log("Lambda Warm-up Offset seconds:", lambdaWarmupOffset);

module.exports = function(target, payload, opts = {}) {
	payload.__cron.time += lambdaWarmupOffset * 1000;
	if (lambdaWarmupOffset <= 0) {
		delete payload.__cron.time;
	}
	let params = {
		FunctionName: target.lambdaName,
		InvocationType: 'RequestResponse',
		Payload: JSON.stringify(payload),
		Qualifier: target.qualifier
	};

	let region = opts.region || process.env.AWS_DEFAULT_REGION;
	var match = params.FunctionName.match(/^arn:aws:lambda:(.*?):/)
	if (match) {
		region = match[1];
	}

	let lambdaApi = new aws.Lambda({
		region: region
	});
	return new Promise((resolve, reject) => {
		logger.log(params);
		let timeoutHandle;
		let request = lambdaApi.invoke(params, function(err, data) {
			timeoutHandle && clearTimeout(timeoutHandle);
			// RequestResponse needs a callback otherwise it doesn't actaully invoke
			console.log(payload.botId, params.FunctionName, "Responded", err, data);
		});
		timeoutHandle = setTimeout(() => request.abort(), 30000);
		resolve({});
	});
};
