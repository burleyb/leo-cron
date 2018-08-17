module.exports = {
	"Records": [{
		"eventID": "5a8c07e96de2dce30c1e02a0fc18054d",
		"eventName": "MODIFY",
		"eventVersion": "1.1",
		"eventSource": "aws:dynamodb",
		"awsRegion": "us-west-2",
		"dynamodb": {
			"ApproximateCreationDateTime": 1487351700,
			"Keys": {
				"id": {
					"S": "Mapper_Step_1"
				}
			},
			"NewImage": {
				"checkpoints": {
					"M": {
						"read": {
							"M": {}
						},
						"write": {
							"M": {}
						}
					}
				},
				"id": {
					"S": "order_generator_v2"
				},
				"instances": {
					"M": {}
				},
				"lambda": {
					"M": {
						"settings": {
							"L": [{
								"M": {
									"destination": {
										"S": "Order"
									}
								}
							}]
						}
					}
				},
				"lambdaName": {
					"S": "DevClintTest-OrderGenerator-I7G2U5U6W2J0"
				},
				"name": {
					"S": "Order Generator v2"
				},
				"paused": {
					"BOOL": true
				},
				"requested_kinesis": {
					"M": {}
				},
				"trigger": {
					"N": Date.now()
				}
			},
			"OldImage": {
				"checkpoints": {
					"M": {
						"read": {
							"M": {}
						},
						"write": {
							"M": {}
						}
					}
				},
				"id": {
					"S": "order_generator_v2"
				},
				"instances": {
					"M": {}
				},
				"lambda": {
					"M": {
						"settings": {
							"L": [{
								"M": {
									"destination": {
										"S": "Order"
									}
								}
							}]
						}
					}
				},
				"lambdaName": {
					"S": "DevClintTest-OrderGenerator-I7G2U5U6W2J0"
				},
				"name": {
					"S": "Order Generator v2"
				},
				"paused": {
					"BOOL": true
				},
				"requested_kinesis": {
					"M": {}
				}
			},
			"SequenceNumber": "573329500000000002303774218",
			"SizeBytes": 1127,
			"StreamViewType": "NEW_AND_OLD_IMAGES"
		},
		"eventSourceARN": "arn:aws:dynamodb:us-west-2:134898387190:table/Leo_cron/stream/2016-12-13T23:06:55.647"
	}]
}