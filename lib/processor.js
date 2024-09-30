"use strict";

import { DynamoDBClient, Converter } from "@aws-sdk/client-dynamodb";
import logger from "leo-logger";
import diff from "deep-diff";
import cronLib from "./utils.js";

const dynamodb = new DynamoDBClient({ region: process.env.AWS_REGION });

export default function(options = {}) {
	const invoke = cronLib.invoke(options);
	const backoff = cronLib.backoff(options);
	const recordError = cronLib.recordError(options);

	let shouldRun = cronLib.shouldRun;
	if (options.shouldRun) {
		shouldRun = (oldImage, newImage) => {
			return options.shouldRun(oldImage, newImage, cronLib.shouldRun);
		};
	}
	let getTarget = cronLib.getTarget;
	if (options.getTarget) {
		getTarget = (oldImage, newImage) => {
			return options.getTarget(oldImage, newImage, cronLib.getTarget);
		};
	}
	let buildPayload = cronLib.buildPayload;
	if (options.buildPayload) {
		buildPayload = (newImage, oldImage, opts) => {
			return options.buildPayload(newImage, oldImage, opts, cronLib.buildPayload);
		};
	}

	const preprocess = options.preprocess || ((a) => a);

	return async function(event, context, done) {
		context.callbackWaitsForEmptyEventLoop = false;

		let records = {};
		const length = event.Records.length - 1;

		// Get the newest and oldest records from the batch
		for (let i = length; i >= 0; i--) {
			const record = event.Records[i];
			const id = record.dynamodb.Keys.id.S;

			if (!records[id]) {
				records[id] = {
					newImage: record.dynamodb.NewImage
						? preprocess(Converter.unmarshall(record.dynamodb.NewImage), "new")
						: { id, trigger: 0, invokeTime: 0 },
				};
			}
			records[id].oldImage = record.dynamodb.OldImage
				? preprocess(Converter.unmarshall(record.dynamodb.OldImage), "old")
				: { id, trigger: 0, invokeTime: 0 };
		}

		const keys = Object.keys(records);
		for (let ndx = 0; ndx < keys.length; ndx++) {
			const record = records[keys[ndx]];
			const { newImage, oldImage } = record;
			const recId = ndx + 1;

			try {
				console.log(`Starting Record ${recId}/${keys.length}`, newImage.id);

				// Remove large log objects before performing diff
				let newLog;
				if (newImage.instances?.[0]?.log !== undefined) {
					newLog = newImage.instances[0].log;
					delete newImage.instances[0].log;
				}
				if (oldImage.instances?.[0]?.log !== undefined) {
					delete oldImage.instances[0].log;
				}

				// Log the differences between the new and old images
				const diffArray = diff(oldImage, newImage) || [];
				const diffs = diffArray.map(
					(e) => `${e.path.join(".")}: ${e.lhs || (e.item && e.item.lhs)}, ${e.rhs || (e.item && e.item.rhs)}`
				);
				logger.log(newImage.id, "Changes", JSON.stringify(diffs, null, 2));

				// Restore the log after diffing
				if (newLog !== undefined) {
					newImage.instances[0].log = newLog;
				}

				if ((newImage.errorCount || 0) > (oldImage.errorCount || 0)) {
					logger.log("Updating backoff", newImage.id, newImage.scheduledTrigger, newImage.errorCount, oldImage.errorCount);
					await backoff(newImage, oldImage, options.backoff);
				} else {
					const result = await shouldRun(newImage, oldImage);

					if (result.passes) {
						if (result.timedout) {
							console.log(newImage.id, "Recording Timeout Error");
							await recordError(newImage, oldImage);
						}

						const payload = await buildPayload(newImage, oldImage, result);
						const target = getTarget(newImage, oldImage);
						const invokeResult = await invoke(target, newImage, oldImage, payload);
						logger.log(invokeResult);
					}
				}
			} catch (e) {
				console.log(newImage.id, e);
			}
			console.log(`Ending Record ${recId}/${keys.length}`);
		}

		// Allow for all invocations to complete
		await new Promise((resolve) => setTimeout(resolve, 100));
		console.log("Exiting");
		done();
	};
}
