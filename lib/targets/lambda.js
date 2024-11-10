import { LambdaClient, InvokeAsyncCommand, InvokeCommand } from "@aws-sdk/client-lambda";
import leoLogger from "leo-logger";

const logger = leoLogger.sub(("[cron/lib/targets/lambda]"));

const lambdaWarmupOffset = process.env.lambda_warmup_offset !== undefined ? parseInt(process.env.lambda_warmup_offset) : 0;
logger.log("Lambda Warm-up Offset seconds:", lambdaWarmupOffset);

export default async function invokeLambda(target, payload, opts = {}) {
    // Adjust the cron time based on lambda warmup offset
    payload.__cron = payload.__cron || {};
    payload.__cron.time = (payload.__cron.time || 0) + lambdaWarmupOffset * 1000;

    if (lambdaWarmupOffset <= 0) {
        delete payload.__cron.time;
    }

    const params = {
        FunctionName: target.lambdaName,
        InvocationType: 'Event',
        Payload: Buffer.from(JSON.stringify(payload)),
        Qualifier: target.qualifier,
        LogType:  'Tail'
    };
    
    if(!target.qualifier) delete params.Qualifier;

    // Resolve region
    let region = opts.region || process.env.AWS_DEFAULT_REGION || "us-east-1";
    const match = params.FunctionName.match(/^arn:aws:lambda:(.*?):/);
    if (match) {
        region = match[1];
    }

    // Initialize AWS SDK Lambda client
    const lambdaApi = new LambdaClient({ 
        region, 
        useFipsEndpoint: false, 
        useDualstackEndpoint: true, 
        logger: logger, 
        maxAttempts: 3, 
        customUserAgent: `${payload.botId} ${params.FunctionName}` 
    });

    try {
        logger.log("[params]", params);

        const data = await lambdaApi.send(new InvokeCommand(params));

        logger.log(payload.botId, params.FunctionName, "==== Responded ====", null, data);

        return {};
    } catch (err) {
        logger.error(payload.botId, params.FunctionName, "==== Invocation failed ====", err);
        return {};
    }
}
