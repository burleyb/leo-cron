var http = require("http");
var https = require("https");
var URL = require("url");

let logger = require("leo-logger")("leo-cron").sub("url");
module.exports = function(target, payload, opts) {
	payload.__cron.time += 4 * 1000;
	var api = http;
	if (target.url.match(/^https/)) {
		api = https;
	}
	opts = Object.assign(URL.parse(target.url), {
		method: target.method || "POST",
		headers: {
			'Content-Type': target.contentType || 'application/json',
		},
		timeout: target.timeout || 1000 * 60 * 10
	});

	logger.log("URL", target.url);
	return new Promise((resolve, reject) => {
		var req = api.request(opts, (res) => {
			var result = "";
			res.on("data", (chunk) => result += chunk);
			res.on("end", () => {
				try {
					let contentType = res.headers["content-type"] || res.headers["Content-Type"] || 'application/json';
					let r = contentType.match(/(?:application|text)\/json/) ? JSON.parse(result) : result;
					resolve(r)
				} catch (err) {
					reject({
						response: result,
						message: err.message
					});
				}
			});
		}).on("error", (err) => {
			reject({
				message: err.message
			});
		});

		if (opts.method === "POST" && payload != undefined) {
			req.write(JSON.stringify(payload));
		}
		req.end();
	});
};