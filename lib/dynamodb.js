const aws = require("aws-sdk");
var https = require("https");

module.exports = function(configure) {
	configure = configure || {};

	var docClient = new aws.DynamoDB.DocumentClient({
		region: configure.region || process.env.AWS_DEFAULT_REGION,
		maxRetries: 2,
		convertEmptyValues: true,
		httpOptions: {
			connectTimeout: 2000,
			timeout: 5000,
			agent: new https.Agent({
				ciphers: 'ALL',
				secureProtocol: 'TLSv1_method',
			})
		},
		credentials: configure.credentials
	});
	return {
		docClient: docClient,

		get: function(table, id, opts, callback) {
			if (!callback) {
				callback = opts;
				opts = {};
			}
			docClient.get({
				TableName: table,
				Key: {
					[opts.id || 'id']: id
				},
				ConsistentRead: true,
				"ReturnConsumedCapacity": 'TOTAL'
			}, function(err, data) {
				if (err) {
					console.log(err);
					callback(err);
				} else {
					callback(null, data.Item);
				}
			});
		},

		put: function(table, id, item, opts, callback) {
			if (!callback) {
				callback = opts;
				opts = {};
			}
			item[opts.id || 'id'] = id;
			docClient.put({
				TableName: table,
				Key: {
					[opts.id || 'id']: id
				},
				Item: item,
				"ReturnConsumedCapacity": 'TOTAL'
			}, function(err) {
				if (err) {
					console.log(err);
					callback(err);
				} else {
					callback(null, "Success");
				}
			});
		},

		merge: function(table, id, obj, opts, callback) {
			if (!callback) {
				callback = opts;
				opts = {};
			}
			this.get(table, id, opts, (err, data) => {
				if (err) {
					return callback(err);
				}
				var data = extend(true, data, obj);
				this.put(table, id, data, opts, callback)
			});
		},
		saveSetting: function(setting_id, value, callback) {
			this.put(configure.resources.LeoSettings, setting_id, {
				value: value
			}, callback);
		},
		getSetting: function(setting_id, callback) {
			this.get(configure.resources.LeoSettings, setting_id, {}, callback);
		},
		update: function(table, key, set, opts, callback) {
			if (!callback) {
				callback = opts;
				opts = {};
			}
			var sets = [];
			var names = {};
			var attributes = {};

			for (var k in set) {
				if (set[k] != undefined) {
					var fieldName = k.replace(/[^a-z]+/ig, "_");
					var fieldOpts = opts.fields && opts.fields[k] || {};
					if (fieldOpts.once) {
						sets.push(`#${fieldName} = if_not_exists(#${fieldName}, :${fieldName})`);
					} else {
						sets.push(`#${fieldName} = :${fieldName}`);
					}
					names[`#${fieldName}`] = k;
					attributes[`:${fieldName}`] = set[k];
				}
			}

			if (Object.keys(attributes) == 0) {
				attributes = undefined;
			}
			if (Object.keys(names) == 0) {
				names = undefined;
			}

			var command = {
				TableName: table,
				Key: key,
				UpdateExpression: sets.length ? 'set ' + sets.join(", ") : undefined,
				ExpressionAttributeNames: names,
				ExpressionAttributeValues: attributes,
				"ReturnConsumedCapacity": 'TOTAL'
			};
			if (opts.ReturnValues) {
				command.ReturnValues = opts.ReturnValues;
			}
			docClient.update(command, callback);
		},
		updateMulti: function(items, opts, callback) {
			if (!callback) {
				callback = opts;
				opts = {};
			}
			opts = Object.assign({
				limit: 20
			}, opts);

			var funcs = [];
			items.forEach((item) => {
				funcs.push((done) => {
					this.update(item.table, item.key, item.set, opts, done);
				});
			});
			async.parallelLimit(funcs, opts.limit, callback);
		},
		scan: function(table, filter, callback) {
			docClient.scan({
				TableName: table,
				"ReturnConsumedCapacity": 'TOTAL'
			}, function(err, data) {
				if (err) {
					console.log(err);
					callback(err);
				} else {
					callback(null, data.Items);
				}
			});
		},
		query: function query(params, configuration, stats) {
			var config = Object.assign({}, {
				mb: 2,
				count: null,
				method: "query",
				progress: function(data, stats, callback) {
					callback(true);
					return true;
				}
			}, configuration);
			stats = Object.assign({}, {
				mb: 0,
				count: 0
			}, stats);
			let method = config.method == "scan" ? "scan" : "query";
			var deferred = new Promise((resolve, reject) => {
				//console.log(params);
				docClient[method](params, function(err, data) {
					if (err) {
						reject(err);
					} else {
						stats.mb++;
						stats.count += data.Count;
						//console.log(config, stats)
						config.progress(data, stats, function(shouldContinue) {
							shouldContinue = shouldContinue == null || shouldContinue == undefined || shouldContinue;
							if (shouldContinue && data.LastEvaluatedKey && stats.mb < config.mb && (config.count == null || stats.count < config.count)) {
								//console.log("Running subquery with start:", data.LastEvaluatedKey)
								params.ExclusiveStartKey = data.LastEvaluatedKey;
								query(params, config, stats).then(function(innerData) {
									data.Items = data.Items.concat(innerData.Items)
									data.ScannedCount += innerData.ScannedCount;
									data.Count += innerData.Count;
									data.LastEvaluatedKey = innerData.LastEvaluatedKey
									if (data.ConsumedCapacity && innerData.ConsumedCapacity) {
										data.ConsumedCapacity.CapacityUnits += innerData.ConsumedCapacity.CapacityUnits;
									}
									data._stats = innerData._stats;
									resolve(data)
								}).catch(function(err) {
									reject(err);
								});

							} else {
								data._stats = stats;
								resolve(data);
							}
						})

					}
				});
			});

			return deferred;
		},
		batchTableWrite: function(table, records, callback) {
			console.log(`Sending ${records.length} records`);
			var request = {
				RequestItems: {},
				"ReturnConsumedCapacity": 'TOTAL'
			};
			request.RequestItems[table] = records;
			docClient.batchWrite(request, function(err, data) {
				if (err) {
					console.log(`All ${records.length} records failed`, err);
					callback(err, records);
				} else if (table in data.UnprocessedItems && Object.keys(data.UnprocessedItems[table]).length !== 0) {
					console.log(`Unprocessed ${data.UnprocessedItems[table].length} records`);
					callback("unprocessed records", data.UnprocessedItems[table]);
				} else {
					callback(null, []);
				}
			});
		},
	};
};