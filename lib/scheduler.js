import moment from "moment";
import later from "later";
import async from "async";
import loggerClient from "leo-logger";
import dynamodbClient from "./dynamodbasync.js";

import leoLogger from "leo-logger";
const logger = leoLogger.sub("[cron/lib/scheduler]")

module.exports = function (options = {}) {

	let updateQueue = async.queue(function (trigger, done) {
		let opts = trigger.opts;
		let id = trigger.id;
		let data = trigger.data;
		let callback = (err) => {
			trigger.callback(err);
			done();
		}

		if (opts.merge === true) {
			dynamodb.merge(CRON_TABLE, id, data, opts, (err) => {
				if (err) {
					logger.error(id, err)
				}
				callback(err)
			});
		} else {
			dynamodb.update(CRON_TABLE, {
				id: id
			}, data, opts, (err) => {
				if (err) {
					logger.error(id, err)
				}
				callback(err)
			});
		}
	}, 20);

	let CRON_TABLE = options.LeoCron || process.env.LeoCron;
	let LAST_SHUTDOWN_TIME = options.shutdown_time_key || "Leo_cron_last_shutdown_time";

	let last_setup_time = Date.now();

	let pollDuration = Object.assign({
		seconds: 30
	}, options.pollDuration);

	let preprocess = typeof options.preprocess === "function" ? options.preprocess : (a => a);

	var dynamodb = dynamodbClient({
		region: options.region || process.env.AWS_DEFAULT_REGION,
		resources: {
			LeoSettings: options.LeoSettings || process.env.LeoSettings
		}
	});

	return function (event, context, done) {
		let duration = Math.ceil((context.getRemainingTimeInMillis() - 1000) / 1000) * 1000;
		let pollInterval;

		// Setup the finish timeout
		setTimeout(function () {
			logger.log("Cron Shutting Down")
			pollInterval && clearInterval(pollInterval);
			for (var k in timeouts) {
				let t = timeouts[k];
				t.interval && t.interval.clear();
				t.timeout && clearTimeout(t.timeout);
				logger.log(`Ending ${k}`);
			}
			updateQueue.kill();
			dynamodb.saveSetting(LAST_SHUTDOWN_TIME, moment.now())
			.then(() => {
				done();
			});
		}, context.getRemainingTimeInMillis() * .95);

		var timeouts = {};
		dynamodb.getSetting(LAST_SHUTDOWN_TIME)
		.then((time) => {
			logger.log("=== Got time setting  ===", time);
			var last_shutdown_time = (!time) ? moment() : moment(time.value);
			if (moment() - last_shutdown_time > duration) {
				last_shutdown_time = moment();
			}

			setup(timeouts, last_shutdown_time, (err) => {
				if (err) {
					logger.log("Error setting up timers", err);
				}
			});
			logger.log("Poll Milliseconds", moment.duration(pollDuration).asMilliseconds());
			pollInterval = setInterval(() => {
				logger.log("Refreshing Timers");
				setup(timeouts, moment(), (err, data) => {
					if (err) {
						logger.log("Error refreshing timers", err);
					}
				});
			}, moment.duration(pollDuration).asMilliseconds());
		});
	}

	function setup (timeouts, last_shutdown_time, done) {
		// Query the table
		dynamodb.query({
			TableName: CRON_TABLE
		}, {
			method: "scan",
			mb: 10
		}).then(function (results) {
			logger.log("Got Cron Table", process.memoryUsage());
			// Set a time out for each cron
			results.Items.forEach(e => {
				try {
					e = preprocess(e);
					let invocation = e.invocation && e.invocation.type && e.invocation.type != "none";
					var eid = e.id;
					var existing = timeouts[eid];
					//logger.log(e.scheduledTrigger, e.scheduledTrigger > Date.now(), timeouts[`scheduled~~${eid}`])
					if (invocation &&
						//!e.paused && 
						e.scheduledTrigger && e.scheduledTrigger > last_setup_time && !timeouts[`scheduled~~${eid}`]) {
						logger.log(`${eid}: Setting up scheduled Trigger ${moment(e.scheduledTrigger).format()}`);
						timeouts[`scheduled~~${eid}`] && clearTimeout(timeouts[`scheduled~~${eid}`].timeout);
						timeouts[`scheduled~~${eid}`] = {
							timeout: setTimeout(() => {
								delete timeouts[`scheduled~~${eid}`];
								setTrigger(eid, {
									trigger: moment.now(),
									scheduledTrigger: 0
								}, function () {
									logger.log(`${eid}: Setting Trigger Status from Scheduled Trigger`)
								});
							}, e.scheduledTrigger - Date.now())
						};
					}

					if (existing) {
						if (e.time == existing.time && invocation && !e.archived) {
							return;
						}
						existing.interval.clear();
						delete timeouts[eid];
						var msg = "";
						if (e.archived) {
							msg = `, archived`
						}
						if (!invocation) {
							msg += ", empty invocation"
						}
						logger.log(eid, e.name, `: removed ${existing.time}${msg}`);
					}
					if (invocation && !e.archived) {
						if (e.time) {
							var parser = (e.time.match(/[a-z]/ig)) ? "text" : "cron";
							var sched = later.parse[parser](e.time, (parser != "cron" || e.time.split(" ").length > 5) ? true : false);

							// Check for missed trigger while restarting
							var prev = Math.max(later.schedule(sched).prev().valueOf(), e.scheduledTrigger || 0);
							if (prev >= last_shutdown_time && !e.paused && prev <= Date.now()) {
								setTrigger(eid, {
									trigger: moment.now()
								}, function () {
									logger.log(`${eid}: Setting Missed Trigger Status - Missed: ${prev} >= Last Shutdown: ${last_shutdown_time}`)
								});
							}

							later.schedule(sched).next(60);
							logger.log(eid, e.name, `: added ${e.time}`);
							timeouts[eid] = {
								time: e.time,
								interval: later.setInterval(function () {
									if (!timeouts[`scheduled~~${eid}`]) {
										setTrigger(eid, {
											trigger: moment.now()
										}, function () {
											logger.log(`${eid}: Setting Trigger Status`)
										});
									}
								}, sched)
							};
						} else if (!e.paused && e.scheduledTrigger && e.scheduledTrigger >= last_shutdown_time && e.scheduledTrigger <= Date.now()) {
							setTrigger(eid, {
								trigger: moment.now()
							}, function () {
								logger.log(`${eid}: Setting Missed Trigger Status - Missed: ${e.scheduledTrigger} >= Last Shutdown: ${last_shutdown_time}`)
							});
						}
					}
				} catch (ex) {
					logger.error(e)
					logger.error(e.id, ex);
				}
			});
			last_setup_time = Date.now();
			done();
		}).catch(done);
	}

	function setTrigger (id, data, opts, callback) {
		if (typeof opts === "function") {
			callback = opts;
			opts = {};
		}
		updateQueue.push({
			id,
			data,
			opts,
			callback
		});
	}
}
