module.exports = {
	preprocess: (image, type) => {
		if (!image.invocation && image.lambdaName) {
			image.invocation = {
				type: "lambda",
				lambdaName: image.lambdaName
			}
		}

		// Move legacy instance data to top level location
		if (image.instances) {
			delete image.invokeTime;
		}
		if (image.instances && image.instances[0]) {
			let i = image.instances[0];
			image.startTime = i.startTime;
			image.endTime = i.completedTime;
			image.maxDuration = i.maxDuration;
			image.status = i.status;
			image.invokeTime = i.invokeTime;
		}

		if (image.fanout) {
			image.fanout = typeof image.fanout === "object" ? image.fanout : {};
			image.fanout = Object.assign(image.fanout, image.instances);
		}

		// Assumes the first trigger is the primary trigger
		let primaryTrigger = (image.triggers || [])[0];

		if (primaryTrigger) {
			image.primaryTrigger = primaryTrigger
			// Move requested queue location & trigger time into goals
			image.goals = Object.assign({}, image.goals, {
				queue_position: image.requested_kinesis && image.requested_kinesis[primaryTrigger]
			});

			if (type == "new") {
				// Move primary checkpoint into progress
				image.progress = Object.assign({}, image.progress, {
					queue_position: image.checkpoints && image.checkpoints.read && image.checkpoints.read[primaryTrigger] && image.checkpoints.read[primaryTrigger].checkpoint
				});
			}
		}

		image.botId = image.id
		image.lock = true;

		// Move legacy settings to the top level
		if (image.lambda && image.lambda.settings && image.lambda.settings[0]) {
			image = Object.assign({}, image.lambda.settings[0], image)
		}

		// Remove legacy fields
		delete image.lambdaName;
		delete image.lambda;
		delete image.invocationType;
		delete image.executionType;
		//delete image.checkpoints;
		delete image.health;
		delete image.templateId;

		//console.log(JSON.stringify(image, null, 2));
		return image;
	},
	buildPayload: (newImage, oldImage, opts, base) => {
		return base(newImage, oldImage, opts).then(p => {
			if (p.__cron.cploc == "fanout") {
				p.__cron.cploc = "instances";
			}
			p.__cron.checkpoints = newImage.checkpoints;

			delete p.checkpoints;
			delete p.primaryTrigger;
			return p;
		});
	},
	createLock: (command, newImage) => {
		//command.UpdateExpression = command.UpdateExpression.replace(", #progress= :progress", "");

		if (!command.ExpressionAttributeNames["#fanout"]) {
			let sets = `set #instances.#index = :value,`;
			command.UpdateExpression = command.UpdateExpression.replace(/^set /, sets);
			command.ExpressionAttributeNames["#instances"] = "instances";
			command.ExpressionAttributeNames["#index"] = "0";
			command.ExpressionAttributeValues[":value"] = {
				token: command.ExpressionAttributeValues[":token"],
				requestId: undefined,
				startTime: undefined,
				invokeTime: command.ExpressionAttributeValues[":invokeTime"],
				status: (newImage && newImage.instances && newImage.instances[0] && newImage.instances[0].status) || null
			};
		} else {
			command.ExpressionAttributeNames["#fanout"] = "instances";
		}

		// Update the checkpoint with the completed fanout checkpoint data
		if (newImage.fanout && newImage.fanout.newCheckpoint) {
			command.UpdateExpression = command.UpdateExpression.replace(/^set /, `set #checkpoints.#read.#targetCP = :targetCP, `);
			command.ExpressionAttributeNames["#checkpoints"] = "checkpoints";
			command.ExpressionAttributeNames["#read"] = "read";
			command.ExpressionAttributeNames["#targetCP"] = newImage.primaryTrigger;
			command.ExpressionAttributeValues[":targetCP"] = newImage.fanout.newCheckpoint;
			delete newImage.fanout.newCheckpoint;
		}

		//delete command.ExpressionAttributeNames["#progress"];
		//delete command.ExpressionAttributeValues[":progress"];
		return command;
	},
	releaseLockOnError: (command) => {
		let sets = `set #instances.#index.#completedTime = :endTime, #instances.#index.#status = :status, #instances.#index.#log = :log`;
		command.UpdateExpression = command.UpdateExpression.replace(/^set /, sets);
		command.ExpressionAttributeNames["#instances"] = "instances";
		command.ExpressionAttributeNames["#index"] = "0";

		return command;
	}
}