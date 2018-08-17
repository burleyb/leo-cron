let aws = require("aws-sdk");
let logger = require("leo-logger")("leo-cron").sub("lambda");
module.exports = function(target, payload, opts = {}) {
	payload.__cron.time += 4 * 1000;
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
		let request = lambdaApi.invoke(params, function(err, data) {
			// RequestResponse needs a callback otherwise it doesn't actaully invoke
			console.log(payload.botId, params.FunctionName, "Responded", err, data);
		});
		setTimeout(() => request.abort(), 2000);
		resolve({});
	});
};