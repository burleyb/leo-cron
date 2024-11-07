import http from "http";
import https from "https";
import { URL } from "url";
import logger from "leo-logger";

logger.sub("leo-cron").sub("url");

export default async function(target, payload, opts = {}) {
  // Adjust the cron time in payload
  payload.__cron.time += 4 * 1000;

  const api = target.url.startsWith("https") ? https : http;
  const url = new URL(target.url);
  const options = {
    ...opts,
    hostname: url.hostname,
    port: url.port,
    path: url.pathname + url.search,
    method: target.method || "POST",
    headers: {
      'Content-Type': target.contentType || 'application/json',
    },
    timeout: target.timeout || 1000 * 60 * 10,
  };

  logger.log("URL", target.url);

  // Return a promise that resolves with the response
  return new Promise((resolve, reject) => {
    const req = api.request(options, (res) => {
      let result = "";

      // Collect response data
      res.on("data", (chunk) => result += chunk);
      res.on("end", () => {
        try {
          const contentType = res.headers["content-type"] || res.headers["Content-Type"] || "application/json";
          const parsedResult = contentType.includes("json") ? JSON.parse(result) : result;
          resolve(parsedResult);
        } catch (err) {
          reject({
            response: result,
            message: err.message
          });
        }
      });
    });

    req.on("error", (err) => reject({ message: err.message }));

    if (options.method === "POST" && payload !== undefined) {
      req.write(JSON.stringify(payload));
    }
    req.end();
  });
}
