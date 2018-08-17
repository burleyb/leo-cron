let legacy = require("../../lib/legacy.js");
exports.handler = require("../../lib/processor.js")(Object.assign({
	region: process.env.AWS_DEFAULT_REGION,
	tableName: process.env.LeoCron
}, legacy));