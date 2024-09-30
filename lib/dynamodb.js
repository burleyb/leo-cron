import { DynamoDBClient, DynamoDBDocumentClient, GetCommand, PutCommand, UpdateCommand, ScanCommand, BatchWriteCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import https from "https";
import merge from "lodash.merge";
import async from "async";

export default function(configure = {}) {
	const dynamoDBClient = new DynamoDBClient({
		region: configure.region || process.env.AWS_DEFAULT_REGION,
		maxAttempts: 2,
		requestHandler: new https.Agent({ ciphers: "ALL" }),
		credentials: configure.credentials,
	});

	const docClient = DynamoDBDocumentClient.from(dynamoDBClient, {
		marshallOptions: { convertEmptyValues: true },
		convertEmptyValues: true,
	});

	return {
		docClient: docClient,

		get: async function(table, id, opts = {}) {
			try {
				const command = new GetCommand({
					TableName: table,
					Key: {
						[opts.id || "id"]: id,
					},
					ConsistentRead: true,
				});
				const data = await docClient.send(command);
				return data.Item;
			} catch (err) {
				console.error(err);
				throw err;
			}
		},

		put: async function(table, id, item, opts = {}) {
			item[opts.id || "id"] = id;
			try {
				const command = new PutCommand({
					TableName: table,
					Item: item,
				});
				await docClient.send(command);
				return "Success";
			} catch (err) {
				console.error(err);
				throw err;
			}
		},

		merge: async function(table, id, obj, opts = {}) {
			try {
				const data = await this.get(table, id, opts);
				const mergedData = merge({}, data, obj);
				await this.put(table, id, mergedData, opts);
			} catch (err) {
				throw err;
			}
		},

		saveSetting: async function(setting_id, value) {
			await this.put(configure.resources.LeoSettings, setting_id, { value });
		},

		getSetting: async function(setting_id) {
			return await this.get(configure.resources.LeoSettings, setting_id);
		},

		update: async function(table, key, set, opts = {}) {
			const sets = [];
			const names = {};
			const attributes = {};

			for (const k in set) {
				if (set[k] !== undefined) {
					const fieldName = k.replace(/[^a-z]+/ig, "_");
					const fieldOpts = (opts.fields && opts.fields[k]) || {};
					sets.push(fieldOpts.once ? `#${fieldName} = if_not_exists(#${fieldName}, :${fieldName})` : `#${fieldName} = :${fieldName}`);
					names[`#${fieldName}`] = k;
					attributes[`:${fieldName}`] = set[k];
				}
			}

			try {
				const command = new UpdateCommand({
					TableName: table,
					Key: key,
					UpdateExpression: sets.length ? `set ${sets.join(", ")}` : undefined,
					ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
					ExpressionAttributeValues: Object.keys(attributes).length ? attributes : undefined,
				});
				if (opts.ReturnValues) {
					command.ReturnValues = opts.ReturnValues;
				}
				await docClient.send(command);
			} catch (err) {
				console.error(err);
				throw err;
			}
		},

		updateMulti: async function(items, opts = {}) {
			opts = Object.assign({ limit: 20 }, opts);

			const funcs = items.map((item) => async () => {
				await this.update(item.table, item.key, item.set, opts);
			});

			return await async.parallelLimit(funcs, opts.limit);
		},

		scan: async function(table) {
			try {
				const command = new ScanCommand({
					TableName: table,
				});
				const data = await docClient.send(command);
				return data.Items;
			} catch (err) {
				console.error(err);
				throw err;
			}
		},

		query: async function(params, config = {}, stats = {}) {
			const defaultConfig = {
				mb: 2,
				count: null,
				method: "query",
				progress: (data, stats, callback) => callback(true),
			};

			config = Object.assign(defaultConfig, config);
			stats = Object.assign({ mb: 0, count: 0 }, stats);

			try {
				let method = config.method === "scan" ? ScanCommand : QueryCommand;
				let result = await docClient.send(new method(params));
				stats.mb++;
				stats.count += result.Count;

				let shouldContinue = await config.progress(result, stats);
				if (shouldContinue && result.LastEvaluatedKey && stats.mb < config.mb && (config.count === null || stats.count < config.count)) {
					params.ExclusiveStartKey = result.LastEvaluatedKey;
					let innerData = await this.query(params, config, stats);
					result.Items = result.Items.concat(innerData.Items);
					result.ScannedCount += innerData.ScannedCount;
					result.Count += innerData.Count;
				}
				result._stats = stats;
				return result;
			} catch (err) {
				console.error(err);
				throw err;
			}
		},

		batchTableWrite: async function(table, records) {
			try {
				const command = new BatchWriteCommand({
					RequestItems: {
						[table]: records,
					},
				});
				const data = await docClient.send(command);
				if (data.UnprocessedItems && data.UnprocessedItems[table] && data.UnprocessedItems[table].length > 0) {
					throw new Error("Unprocessed records");
				}
				return [];
			} catch (err) {
				console.error(err);
				throw err;
			}
		},
	};
};
