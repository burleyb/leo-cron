import { LambdaClient, InvokeAsyncCommand, InvokeCommand } from "@aws-sdk/client-lambda";
import leoLogger from "leo-logger";

const logger = leoLogger.sub(("[cron/lib/targets/lambda]"));

const lambdaWarmupOffset = process.env.lambda_warmup_offset !== undefined ? parseInt(process.env.lambda_warmup_offset) : 0;
logger.log("Lambda Warm-up Offset seconds:", lambdaWarmupOffset);

// Create a map to store clients by region
const lambdaClients = new Map();

// Function to get or create client for a region
function getLambdaClient(region) {
    if (!lambdaClients.has(region)) {
        lambdaClients.set(region, new LambdaClient({ 
            region, 
            useFipsEndpoint: false, 
            useDualstackEndpoint: true, 
            logger: logger, 
            maxAttempts: 3
        }));
    }
    return lambdaClients.get(region);
}

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

    // Get existing client or create new one for this region
    const lambdaApi = getLambdaClient(region);

    try {
        logger.log("[params]", params);
        
        const command = new InvokeCommand({
            ...params,
            CustomUserAgent: `${payload.botId} ${params.FunctionName}`
        });

        const data = await lambdaApi.send(command);

        logger.log(payload.botId, params.FunctionName, "==== Responded ====", null, data);

        return {};
    } catch (err) {
        logger.error(payload.botId, params.FunctionName, "==== Invocation failed ====", err);
        return {};
    }
}
