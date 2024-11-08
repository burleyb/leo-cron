import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { 
  DynamoDBDocumentClient, 
  GetCommand, 
  PutCommand, 
  UpdateCommand, 
  ScanCommand, 
  BatchWriteCommand, 
  QueryCommand 
} from "@aws-sdk/lib-dynamodb";
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import https from "https";
import merge from "lodash.merge";
import async from "async";

import leoLogger from "leo-logger";
const logger = leoLogger.sub("[cron/lib/dynamodb]")

module.exports = function(configure) {
	configure = configure || {};

	const client = new DynamoDBClient({
	    region: configure.region || process.env.AWS_DEFAULT_REGION,
	    maxAttempts: 2,
	    logger: logger,
	    requestHandler: new NodeHttpHandler({
	      requestTimeout: 3_000,
	      httpsAgent: new https.Agent({
		maxSockets: 25,
	      }),
	    }),
	    credentials: configure.credentials,
	});

	  const docClient = DynamoDBDocumentClient.from(client, {
	    marshallOptions: { convertEmptyValues: true, removeUndefinedValues: true }
	  });

return {
    docClient: docClient,

    get: function (table, id, opts, callback) {
      if (!callback) {
        callback = opts;
        opts = {};
      }

      docClient
        .send(
          new GetCommand({
            TableName: table,
            Key: { [opts.id || "id"]: id },
            ConsistentRead: true,
            ReturnConsumedCapacity: "TOTAL",
          })
        )
        .then((data) => callback(null, data.Item))
        .catch((err) => {
          console.log(err);
          callback(err);
        });
    },

    put: function (table, id, item, opts, callback) {
      if (!callback) {
        callback = opts;
        opts = {};
      }
      item[opts.id || "id"] = id;

      docClient
        .send(
          new PutCommand({
            TableName: table,
            Item: item,
            ReturnConsumedCapacity: "TOTAL",
          })
        )
        .then(() => callback(null, "Success"))
        .catch((err) => {
          console.log(err);
          callback(err);
        });
    },

    merge: function (table, id, obj, opts, callback) {
      if (!callback) {
        callback = opts;
        opts = {};
      }
      this.get(table, id, opts, (err, data) => {
        if (err) return callback(err);
        const mergedData = merge(data, obj);
        this.put(table, id, mergedData, opts, callback);
      });
    },

    saveSetting: function (setting_id, value, callback) {
      this.put(
        configure.resources.LeoSettings,
        setting_id,
        { value: value },
        callback
      );
    },

    getSetting: function (setting_id, callback) {
      this.get(configure.resources.LeoSettings, setting_id, {}, callback);
    },

    update: function (table, key, set, opts, callback) {
      if (!callback) {
        callback = opts;
        opts = {};
      }

      let sets = [];
      let names = {};
      let attributes = {};

      for (const k in set) {
        if (set[k] !== undefined) {
          const fieldName = k.replace(/[^a-z]+/gi, "_");
          const fieldOpts = (opts.fields && opts.fields[k]) || {};
          if (fieldOpts.once) {
            sets.push(`#${fieldName} = if_not_exists(#${fieldName}, :${fieldName})`);
          } else {
            sets.push(`#${fieldName} = :${fieldName}`);
          }
          names[`#${fieldName}`] = k;
          attributes[`:${fieldName}`] = set[k];
        }
      }

      const command = new UpdateCommand({
        TableName: table,
        Key: key,
        UpdateExpression: sets.length ? "set " + sets.join(", ") : undefined,
        ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
        ExpressionAttributeValues: Object.keys(attributes).length
          ? attributes
          : undefined,
        ReturnConsumedCapacity: "TOTAL",
        ReturnValues: opts.ReturnValues,
      });

      docClient
        .send(command)
        .then((data) => callback(null, data))
        .catch((err) => callback(err));
    },

    updateMulti: function (items, opts, callback) {
      if (!callback) {
        callback = opts;
        opts = {};
      }
      opts = { limit: 20, ...opts };

      const funcs = items.map((item) => (done) =>
        this.update(item.table, item.key, item.set, opts, done)
      );

      async.parallelLimit(funcs, opts.limit, callback);
    },

    scan: function (table, filter, callback) {
      const command = new ScanCommand({
        TableName: table,
        ReturnConsumedCapacity: "TOTAL",
      });

      docClient
        .send(command)
        .then((data) => callback(null, data.Items))
        .catch((err) => {
          console.log(err);
          callback(err);
        });
    },

    query: function query(params, configuration, stats) {
      const config = {
        mb: 2,
        count: null,
        method: "query",
        progress: (data, stats, callback) => {
          callback(true);
          return true;
        },
        ...configuration,
      };
      stats = { mb: 0, count: 0, ...stats };

      return new Promise((resolve, reject) => {
        docClient
          .send(new QueryCommand(params))
          .then((data) => {
            stats.mb++;
            stats.count += data.Count;

            config.progress(data, stats, (shouldContinue) => {
              if (
                shouldContinue !== false &&
                data.LastEvaluatedKey &&
                stats.mb < config.mb &&
                (config.count === null || stats.count < config.count)
              ) {
                params.ExclusiveStartKey = data.LastEvaluatedKey;
                query(params, config, stats)
                  .then((innerData) => {
                    data.Items = data.Items.concat(innerData.Items);
                    data.ScannedCount += innerData.ScannedCount;
                    data.Count += innerData.Count;
                    data.LastEvaluatedKey = innerData.LastEvaluatedKey;
                    if (data.ConsumedCapacity && innerData.ConsumedCapacity) {
                      data.ConsumedCapacity.CapacityUnits +=
                        innerData.ConsumedCapacity.CapacityUnits;
                    }
                    data._stats = innerData._stats;
                    resolve(data);
                  })
                  .catch(reject);
              } else {
                data._stats = stats;
                resolve(data);
              }
            });
          })
          .catch(reject);
      });
    },

    batchTableWrite: function (table, records, callback) {
      console.log(`Sending ${records.length} records`);

      const request = {
        RequestItems: {
          [table]: records,
        },
        ReturnConsumedCapacity: "TOTAL",
      };

      docClient
        .send(new BatchWriteCommand(request))
        .then((data) => {
          if (data.UnprocessedItems && data.UnprocessedItems[table]?.length) {
            console.log(`Unprocessed ${data.UnprocessedItems[table].length} records`);
            callback("unprocessed records", data.UnprocessedItems[table]);
          } else {
            callback(null, []);
          }
        })
        .catch((err) => {
          console.log(`All ${records.length} records failed`, err);
          callback(err, records);
        });
    },
  };
};
