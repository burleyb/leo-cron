"use strict";

let cronLib = require("./utils.js");

let diff = require("deep-diff");
let aws = require("aws-sdk");

let logger = require("leo-logger")("leo-cron");

module.exports = function(options = {}) {
	let invoke = cronLib.invoke(options);
	let backoff = cronLib.backoff(options);
	let recordError = cronLib.recordError(options);

	let shouldRun = cronLib.shouldRun;
	if (options.shouldRun) {
		shouldRun = (oldImage, newImage) => {
			return options.shouldRun(oldImage, newImage, cronLib.shouldRun);
		}
	}
	let getTarget = cronLib.getTarget;
	if (options.getTarget) {
		getTarget = (oldImage, newImage) => {
			return options.getTarget(oldImage, newImage, cronLib.shouldRun);
		}
	}
	let buildPayload = cronLib.buildPayload;
	if (options.buildPayload) {
		buildPayload = (newImage, oldImage, opts) => {
			return options.buildPayload(newImage, oldImage, opts, cronLib.buildPayload);
		}
	}

	let preprocess = options.preprocess || (a => a);

	return async function(event, context, done) {
		// TODO: Make it so it can poll the db stream for changes

		// let streams = await dynamodb.listStreams({
		// 	TableName: options.tableName
		// }).promise();

		// let streamArn = streams.Streams[0].StreamArn
		// let shards = await dynamodb.describeStream({
		// 	StreamArn: streamArn
		// }).promise();
		// logger.log(shards.StreamDescription.Shards);

		// let iterator = await dynamodb.getShardIterator({
		// 	StreamArn: streamArn,
		// 	ShardIteratorType: "AFTER_SEQUENCE_NUMBER",
		// 	ShardId: shardId,
		// 	SequenceNumber: "THE_START_VALUE"
		// }).promise();

		// logger.log(iterator)
		// process.exit();

		context.callbackWaitsForEmptyEventLoop = false;

		let records = {}
		let length = event.Records.length - 1;
		// Get Newest and oldest from the batch for each record
		for (let i = length; i >= 0; i--) {
			let record = event.Records[i];
			let id = record.dynamodb.Keys.id.S;

			if (!(id in records)) {
				records[id] = {
					newImage: record.dynamodb.NewImage ? preprocess(aws.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage), "new") : {
						id: id,
						trigger: 0,
						invokeTime: 0
					}
				}
			}
			records[id].oldImage = record.dynamodb.OldImage ? preprocess(aws.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage), "old") : {
				id: id,
				trigger: 0,
				invokeTime: 0
			}
		}
		let keys = Object.keys(records);
		for (let ndx = 0; ndx < keys.length; ndx++) {
			let record = records[keys[ndx]];
			let newImage = record.newImage;
			let oldImage = record.oldImage;

			let recId = ndx + 1;
			try {
				console.log(`Starting Record ${recId}/${keys.length}`, newImage && newImage.id);

				// Fix bug where log is too large and causes a infinite loop in deepdiff
				let newLog = undefined;
				if (newImage.instances && newImage.instances[0] && newImage.instances[0].log !== undefined) {
					newLog = newImage.instances[0].log;
					delete newImage.instances[0].log;
				}
				if (oldImage.instances && oldImage.instances[0] && oldImage.instances[0].log !== undefined) {
					delete oldImage.instances[0].log;
				}

				// Log what changed
				var diffArray = diff(oldImage, newImage) || [];
				var diffs = (diffArray).map(e => `${e.path.join(".")}: ${e.lhs || (e.item && e.item.lhs)}, ${e.rhs || (e.item && e.item.rhs)}`);
				logger.log(newImage.id, "Changes", JSON.stringify(diffs, null, 2));
				//logger.log(newImage.id, "Record", JSON.stringify(record, null, 2));

				if (newLog !== undefined) {
					newImage.instances[0].log = newLog;
				}

				if ((newImage.errorCount || 0) > (oldImage.errorCount || 0)) {
					// Reported an error. Apply backoff
					logger.log("Updating backoff", newImage.id, newImage.scheduledTrigger, newImage.errorCount || 1, oldImage.errorCount || 1)
					await backoff(newImage, oldImage, options.backoff);
				}
				// TODO: Was causing errors so we removed it for now
				else if (false && !cronLib.makingProgress(newImage, oldImage)) {
					console.log(newImage.id, "Recording No Progress Error")
					await recordError(newImage, oldImage);
				} else {

					let result = await shouldRun(newImage, oldImage);


					if (result.passes) {
						if (result.timedout) {
							try {
								console.log(newImage.id, "Recording Timeout Error");
								await recordError(newImage, oldImage);
							} catch (e) {
								// Ignore reporting the error if there is an error
							}
						}
						result.position = (eid) => {
							if (!eid) {
								return 0;
							}
							let parts = eid.match(/(\d+)/g).map(n => parseInt(n));
							if (parts.length >= 6) {
								return parts[5];
							} else {
								if (parts.length >= 2) {
									parts[1]--; //fix month to be 0 based
								}
								return Date.UTC.apply(null, parts).valueOf();
							}
						}
						let payload = await buildPayload(newImage, oldImage, result);
						let target = getTarget(newImage, oldImage);
						let invokeResult = await invoke(target, newImage, oldImage, payload);
						logger.log(invokeResult)
					}
				}
			} catch (e) {
				console.log(newImage.id, e);
				// TODO: Should we log an error to the bot here?
			}
			console.log(`Ending Record ${recId}/${keys.length}`);
		}

		console.log("Waiting for time");
		// Let all invocations be fired
		await new Promise(resolve => {
			setTimeout(() => {
				console.log("Waiting for time done");
				resolve();
			}, 100);
		});
		console.log("Exiting");
		done();
	}
}
