let aws = require("aws-sdk");
let logger = require("leo-logger")("leo-cron").sub("sns");
module.exports = function(target, payload, opts) {
	payload.__cron.time += 4 * 1000;
	var params = {
		Message: JSON.stringify(payload),
		TargetArn: target.arn
	};

	let region = opts.region || process.env.AWS_DEFAULT_REGION;
	var match = params.TargetArn.match(/^arn:aws:sns:(.*?):/)
	if (match) {
		region = match[1];
	}

	let sns = new aws.SNS({
		region: region
	});
	return new Promise((resolve, reject) => {
		sns.publish(params, function(err, data) {
			if (err) {
				reject(err);
			} else {
				resolve(data);
			}
		});


	})
};