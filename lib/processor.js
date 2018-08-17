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

			try {
				// Log what changed
				var diffArray = diff(oldImage, newImage) || [];
				var diffs = (diffArray).map(e => `${e.path.join(".")}: ${e.lhs || (e.item && e.item.lhs)}, ${e.rhs || (e.item && e.item.rhs)}`);
				logger.log(newImage.id, "Changes", JSON.stringify(diffs, null, 2));
				//logger.log(newImage.id, "Record", JSON.stringify(record, null, 2));

				if ((newImage.errorCount || 0) > (oldImage.errorCount || 0)) {
					// Reported an error. Apply backoff
					logger.log("Updating backoff", newImage.id, newImage.scheduledTrigger, newImage.errorCount || 1, oldImage.errorCount || 1)
					await backoff(newImage, oldImage, options.backoff);
				} else if (!cronLib.makingProgress(newImage, oldImage)) {
					console.log(newImage.id, "Recording No Progress Error")
					await recordError(newImage, oldImage);
				} else {

					let result = await shouldRun(newImage, oldImage);

					if (result.passes) {
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
			}
		}

		// Let all invocations be fired 
		setTimeout(() => {
			done();
		}, 100);
	}
}