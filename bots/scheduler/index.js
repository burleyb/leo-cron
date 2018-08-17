let legacy = require("../../lib/legacy.js");
exports.handler = require("../../lib/scheduler.js")(Object.assign({
	region: process.env.AWS_DEFAULT_REGION,
	LeoCron: process.env.LeoCron,
	LeoSettings: process.env.LeoSettings
}, legacy));