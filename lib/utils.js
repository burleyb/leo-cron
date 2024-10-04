let logger = require("leo-logger")("leo-cron");
const moment = require("moment");
const merge = require("lodash.merge");
const invocationTargets = {
	lambda: require("./targets/lambda"),
	fargate: require("./targets/fargate"),
	sns: require("./targets/sns"),
	url: require("./targets/url"),
};
import dynamodbClient from "./dynamodb.js";


// Used for Spacing
const tabs = "                                                      ";

let util = module.exports = {
	invoke: (options) => {
		const dynamodb = dynamodbClient({
			region: options.region || process.env.AWS_DEFAULT_REGION
		});
		return async function(target, newImage, oldImage, payload) {
			if (!target || !target.type) {
				return "Undefined invocation target"
			}
			let invocationTarget = invocationTargets[target.type];
			if (invocationTarget) {
				let cron = newImage;
				var newInvokeTime = moment.now();
				newImage.progress = newImage.progress || {};
				var command = {
					TableName: options.tableName,
					Key: {
						id: cron.id
					},
					UpdateExpression: 'set #invokeTime = :invokeTime, #token= :token, #progress= :progress remove #ignorePaused, #startTime, #endTime',
					ConditionExpression: '#invokeTime <= :lastInvokeTime',
					ExpressionAttributeNames: {
						"#invokeTime": "invokeTime",
						"#token": "token",
						"#progress": "progress",

						"#startTime": "startTime",
						"#endTime": "endTime",
						"#ignorePaused": "ignorePaused",
					},
					ExpressionAttributeValues: {
						":token": payload.__cron.ts,
						":invokeTime": newInvokeTime,
						":progress": newImage.progress,
						":lastInvokeTime": newImage.invokeTime
					},
					"ReturnConsumedCapacity": 'TOTAL'
				};
				if (newImage.invokeTime == undefined) {
					command.ConditionExpression = 'attribute_not_exists(#invokeTime)';
					delete command.ExpressionAttributeValues[":lastInvokeTime"];
				}

				if (payload.__cron.iids) {
					let updateExp = ["#fanoutSummary.#lastCount = :fanoutCount", "#fanoutSummary.#lastMax = :fanoutMax"];
					command.ExpressionAttributeNames["#fanoutSummary"] = "fanout";
					command.ExpressionAttributeNames["#lastCount"] = "lastCount";
					command.ExpressionAttributeNames["#lastMax"] = "lastMax";
					command.ExpressionAttributeValues[":fanoutCount"] = payload.__cron.icount;
					command.ExpressionAttributeValues[":fanoutMax"] = payload.__cron.maxeid;

					payload.__cron.iids.map(iid => {
						updateExp.push(`#fanout.#iid${iid} = :iid${iid}`)
						command.ExpressionAttributeNames["#iid" + iid] = iid.toString();
						command.ExpressionAttributeValues[":iid" + iid] = {
							token: command.ExpressionAttributeValues[":token"],
							requestId: undefined,
							startTime: undefined,
							invokeTime: command.ExpressionAttributeValues[":invokeTime"],
							status: (newImage && newImage.fanout && newImage.fanout[iid] && newImage.fanout[iid].status) || null
						};
						// Keep positions
						let i = newImage && newImage.fanout && newImage.fanout[iid] || {};
						Object.keys(i).map(key => {
							if (key.match(/^(?:queue|system):/)) {
								command.ExpressionAttributeValues[":iid" + iid][key] = i[key];
							}
						})
					});
					command.ExpressionAttributeNames["#fanout"] = "fanout";
					command.UpdateExpression = command.UpdateExpression.replace(/^set /, `set ${updateExp.join(", ")}, `);
				}

				try {
					if (typeof options.createLock === "function") {
						command = options.createLock(command, newImage, oldImage, payload);
					}
					let saveValue = await dynamodb.docClient.update(command).promise();
					cron.invokeTime = newInvokeTime;

					let iids = payload.__cron.iids || ["0"];
					logger.log(`${cron.id} - ${cron.name} - ${iids} Invoking ${JSON.stringify(target)} with params:`)
					logger.log(cron.id, JSON.stringify(payload, null, 2));
					delete payload.__cron.iids;
					let basePayload = payload;
					// TODO: this could be an error.  Should it be coming from payload.instances?
					let instances = payload.__cron.instances || {};
					for (var k = 0; k < iids.length; k++) {
						let iid = iids[k];
						let payload = merge({}, basePayload);
						payload.__cron.iid = iid;
						payload.__cron.instances = {
							[iid]: instances[iid]
						};
						let starts = {}
						let cnt = 0;
						Object.keys(newImage.fanout && newImage.fanout[iid] || {}).map(queue => {
							let data = newImage.fanout[iid][queue];

							if (data && data.checkpoint) {
								starts[queue] = data.checkpoint;
								cnt++;
							}
						});
						if (cnt) {
							payload.__cron.starteid = starts;
						}
						try {
							let data = await invocationTarget(target, payload, {
								region: options.region || process.env.AWS_DEFAULT_REGION
							});
							logger.log(cron.id, payload.__cron.iid, JSON.stringify(data, null, 2));
						} catch (err) {
							try {
								logger.log(`${cron.id} - ${cron.name} - ${payload.__cron.iid} `, err);
								let command = {
									TableName: options.tableName,
									Key: {
										id: cron.id
									},
									UpdateExpression: 'set #endTime = :endTime, #status = :status',
									ConditionExpression: '#token = :token',
									ExpressionAttributeNames: {
										"#endTime": "endTime",
										"#token": "token",
										"#status": "status",
										"#log": "log"
									},
									ExpressionAttributeValues: {
										":endTime": Date.now(),
										":token": payload.__cron.ts,
										":status": status,
										":log": zlib.gzipSync(JSON.stringify(err.message)),
									},
									"ReturnConsumedCapacity": 'TOTAL'
								}
								if (typeof options.releaseLockOnError == "funtion") {
									command = options.releaseLockOnError(command, newImage, oldImage, payload);
								}
								await dynamodb.docClient.update(command).promise();
							} catch (error) {
								logger.log("Error reporting invoke error", error);
							}
						}

					}
				} catch (error) {
					logger.log(`${cron.id} - ${cron.name} - ${payload.__cron.iid}`, error, command);
					logger.log(`${cron.id} - ${cron.name} - ${payload.__cron.iid} Failed to Create Lock`);
				}
			} else {
				return `Invalid invocation target: '${target.type}'`;
			}
		}
	},
	getTarget: (newImage, oldImage) => {
		let target = newImage.invocation;
		let isLambda = target.type == "lambda";
		let isBehindFarBehind = false;
		let hasTimedout = oldImage.timedout;

		if (isLambda && (isBehindFarBehind || hasTimedout)) {
			// switch to fargate
			target = {
				type: "fargate",
				lambdaName: target.lambdaName
			}
		}
		return target;
	},
	buildPayload: (newImage, oldImage, opts) => {
		let payload = merge({}, newImage, {
			__cron: {
				id: newImage.id,
				iid: "0",
				name: newImage.lambdaName,
				ts: newImage.trigger,
				time: Date.now(),
				botName: newImage.name,
			}
		});
		delete payload.invocation;
		delete payload.triggers;
		delete payload.time;
		delete payload.fanout;

		//util.applyFanout(payload, newImage, oldImage, opts);

		return Promise.resolve(payload);
	},
	applyFanout: function(payload, newImage, oldImage, opts = {}) {
		if (newImage.fanout && newImage.fanout.enabled) {
			let config = Object.assign({
				queue_position: 2,
				max: 10,
				min: 1
			}, (typeof newImage.fanout === "object") ? newImage.fanout : {});

			if (config.queue_position && opts.goals.or.queue_position && opts.progress.queue_position) {
				let lastTargetPosition = config.lastMax || "";
				let cp = {
					records: 0,
					checkpoint: config.lastMax,
					started_timestamp: null,
					ended_timestamp: null,
					source_timestamp: null
				};
				let notFinished = Object.keys(config || {}).map((key) => {
					let data = config[key];
					if (config.lastCount && key.match(/\d+/) && typeof data == "object" && (parseInt(key) < config.lastCount)) {
						let q = (data[newImage.primaryTrigger || ""] || {});
						cp.records += (q.records || 0);
						cp.started_timestamp = min(cp.started_timestamp, q.started_timestamp);
						cp.ended_timestamp = max(cp.ended_timestamp, q.ended_timestamp);
						cp.source_timestamp = max(cp.source_timestamp, q.source_timestamp);

						let cpPosition = q.checkpoint || "";
						return (cpPosition >= lastTargetPosition) ? undefined : parseInt(key);
					}
					return undefined;
				}).filter(i => i != undefined);
				if (notFinished.length == 0) {
					if (config.lastCount && cp.records) {
						cp.records /= config.lastCount;
						newImage.fanout.newCheckpoint = cp;
						newImage.progress.queue_position = max(config.lastMax, newImage.progress.queue_position);
					}

					let instanceCount = Math.floor(moment.duration({
						milliseconds: opts.position(opts.goals.or.queue_position) - opts.position(opts.progress.queue_position)
					}).asHours() * config.queue_position);
					if (instanceCount > 1) {
						payload.__cron.cploc = "fanout";
						payload.__cron.icount = Math.max(Math.min(instanceCount, config.max), config.min)
						payload.__cron.maxeid = opts.goals.or.queue_position;
						payload.__cron.iids = Array.apply(null, {
							length: payload.__cron.icount
						}).map(Number.call, Number);
					}
				} else {
					payload.__cron.cploc = "fanout";
					payload.__cron.icount = config.lastCount
					payload.__cron.maxeid = config.lastMax;
					payload.__cron.iids = notFinished;
				}
			}
		}
	},
	checkGoals: function(progress, goals, join = "and", level = 1) {
		let result = {
			value: join == "and",
			tests: ""
		};
		let tests = [];
		let prefix = tabs.slice(0, level * 2); // + level;
		let prefixPrev = tabs.slice(0, Math.max(0, level - 1) * 2);
		return Object.keys(goals).reduce((agg, g) => {
			if ((join == "and" && !agg.value) || (join != "and" && !!agg.value)) {
				return agg;
			}

			let goal = goals[g];
			let comp;
			if (typeof goal == "object") {
				let nextLevel = level + 1
				let label = g + " ";
				if (g == "and" || g == "or") {
					label = "";
				}
				let nested = util.checkGoals(progress, goal, g == "or" ? g : "and", nextLevel);
				comp = nested.value;
				tests.push(`${prefix}${label}(\n${nested.tests}\n${prefix})`);
			} else {
				comp = (progress[g] == undefined && goal != undefined && typeof goal != "boolean") || (typeof goal == "boolean" ? !!progress[g] == goal : progress[g] < goal);
				tests.push(`${prefix}(${g}=${comp} goal:${goal} progress:${progress[g]})`);
			}
			if (join == "and") {
				agg.value = agg.value && comp;
			} else {
				agg.value = agg.value || comp;
			}
			agg.tests = tests.join(` ${join}\n`);
			return agg;
		}, result);
	},
	shouldRun: async function(newImage, oldImage) {
		let now = moment.now();

		var timeout = newImage.maxDuration || 0;
		var invokeTime = newImage.invokeTime || 0;

		// TODO: Handle the same token invocation
		let sameToken = newImage.token == newImage.trigger;


		// Goals
		let goals = {
			not_paused: true,
			is_valid: true,
			not_erroring: true,
			not_already_running: true,
			not_in_backoff: true,
			or: merge({
				is_triggered: newImage.trigger || 0
			}, newImage.goals)
		};

		let progress = merge({
			not_paused: (!newImage.paused || !!newImage.ignorePaused),
			is_valid: (newImage.invocation && newImage.invocation.type && newImage.invocation.type != "none") || false,
			not_in_backoff: (newImage.scheduledTrigger || 0) <= Date.now() || !!newImage.ignorePaused, // It was forced
			not_erroring: ((newImage.errorCount || 0) <= 10 || (Date.now() - invokeTime) >= 900000) || false,
			not_already_running: (!!newImage.endTime || (invokeTime) + (newImage.maxDuration || newImage.timeout || 900000) < now),
			is_triggered: invokeTime,
			error_count: newImage.errorCount || 0,
			error_timeout: (Date.now() - invokeTime) >= 900000
		}, newImage.progress);

		let goalTest = util.checkGoals(progress, goals);
		logger.log(`${newImage.id} - ${newImage.name}\npasses: ${goalTest.value}\ntests:\n${goalTest.tests}`);

		return {
			passes: goalTest.value,
			timedout: invokeTime != 0 && newImage.endTime == null && invokeTime + (newImage.maxDuration || newImage.timeout || 900000) < now,
			goals: goals,
			progress: progress
		}
	},
	hasMoreToDo: function(newImage, oldImage, key) {
		var reads = newImage && newImage.checkpoints && newImage.checkpoints.read || {};
		//logger.log(JSON.stringify(newImage, null, 2));
		let allowed = {}
		if (key) {
			allowed = {
				[key]: true
			};
		} else if (newImage.triggers && !newImage.ignoreHasMore) {
			newImage.triggers.map(q => {
				allowed[q] = true
			});
		}
		return newImage &&
			newImage.requested_kinesis &&
			Object.keys(newImage.requested_kinesis).filter(k => allowed[k]).reduce((result, event) => {
				var latest = newImage.requested_kinesis[event];
				var checkpoint = reads[event];
				//logger.log(`${event}, ${result}, ${JSON.stringify(checkpoint)}, ${latest}`)
				return result || !checkpoint || (latest > checkpoint.checkpoint);
			}, false) || false;
	},
	backoff: (options) => {
		var dynamodb = dynamodbClient({
			region: options.region || process.env.AWS_DEFAULT_REGION
		});
		return async function(newImage, oldImage, backoffFunction) {
			let defaultBackoff = (newImage, oldImage) => ({
				seconds: Math.pow(2.25, Math.min(10, newImage.errorCount || 1))
			});
			backoffFunction = backoffFunction || defaultBackoff;
			newImage.scheduledTrigger = moment().add(moment.duration(backoffFunction(newImage, oldImage, defaultBackoff))).valueOf();
			return dynamodb.docClient.update({
				TableName: options.tableName,
				Key: {
					id: newImage.id
				},
				UpdateExpression: 'set #scheduledTrigger = :scheduledTrigger',
				ExpressionAttributeNames: {
					"#scheduledTrigger": "scheduledTrigger"
				},
				ExpressionAttributeValues: {
					":scheduledTrigger": newImage.scheduledTrigger
				},
			}).promise();
		}
	},
	makingProgress: (newImage, oldImage) => {
		return true;

		// TODO: This is bad logic.
		// If i just finished running
		/*
		if (newImage.endTime && !oldImage.endTime &&
			newImage.progress && oldImage.progress &&
			newImage.primaryTrigger &&
			newImage.progress.queue_position == oldImage.progress.queue_position) {
			return false;
		}
		return true;
		*/
	},
	recordError: (options) => {
		var dynamodb = dynamodbClient({
			region: options.region || process.env.AWS_DEFAULT_REGION
		});
		return async function(newImage, oldImage) {
			return dynamodb.docClient.update({
				TableName: options.tableName,
				Key: {
					id: newImage.id
				},
				UpdateExpression: 'SET #err = :err',
				ExpressionAttributeNames: {
					"#err": "errorCount"
				},
				ExpressionAttributeValues: {
					":err": max(0, newImage.errorCount, oldImage.errorCount) + 1
				},
			}).promise();
		}
	}
};

function max() {
	var max = arguments[0]
	for (var i = 1; i < arguments.length; ++i) {
		if (arguments[i] != null && arguments[i] != undefined) {
			max = max > arguments[i] ? max : arguments[i];
		}
	}
	return max;
}

function min() {
	var min = arguments[0]
	for (var i = 1; i < arguments.length; ++i) {
		if (arguments[i] != null && arguments[i] != undefined) {
			min = min < arguments[i] ? min : arguments[i];
		}
	}
	return min;
}
