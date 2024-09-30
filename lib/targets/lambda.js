import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import logger from "leo-logger";

const lambdaWarmupOffset = process.env.lambda_warmup_offset != undefined ? parseInt(process.env.lambda_warmup_offset) : 0;
logger.sub("lambda").log("Lambda Warm-up Offset seconds:", lambdaWarmupOffset);

export default async function(target, payload, opts = {}) {
    // Adjust the cron time based on lambda warmup offset
    payload.__cron.time += lambdaWarmupOffset * 1000;
    if (lambdaWarmupOffset <= 0) {
        delete payload.__cron.time;
    }

    const params = {
        FunctionName: target.lambdaName,
        InvocationType: 'Event',
        Payload: JSON.stringify(payload),
        Qualifier: target.qualifier
    };

    let region = opts.region || process.env.AWS_DEFAULT_REGION;
    const match = params.FunctionName.match(/^arn:aws:lambda:(.*?):/);
    if (match) {
        region = match[1];
    }

    // Initialize AWS SDK Lambda client
    const lambdaApi = new LambdaClient({ region });

    try {
        logger.log(params);

        const data = await lambdaApi.send(new InvokeCommand(params));

        // Log the invocation response
        console.log(payload.botId, params.FunctionName, "Responded", null, data);

        // Returning an empty object as in the original promise resolution
        return {};
    } catch (err) {
        // Log the error, if there is one
        console.error(payload.botId, params.FunctionName, "Invocation failed", err);
        // You can reject here if needed
        return {};
    }
}
