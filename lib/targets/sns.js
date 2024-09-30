import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import logger from "leo-logger";

logger.sub("sns");

export default async function(target, payload, opts = {}) {
    // Adjust the cron time
    payload.__cron.time += 4 * 1000;

    const params = {
        Message: JSON.stringify(payload),
        TargetArn: target.arn
    };

    let region = opts.region || process.env.AWS_DEFAULT_REGION;
    const match = params.TargetArn.match(/^arn:aws:sns:(.*?):/);
    if (match) {
        region = match[1];
    }

    // Initialize the AWS SDK SNS client
    const snsClient = new SNSClient({ region });

    try {
        logger.log("Publishing message to SNS:", params);

        // Send the SNS message
        const data = await snsClient.send(new PublishCommand(params));

        logger.log("SNS publish successful:", data);

        return data;
    } catch (err) {
        logger.error("SNS publish failed:", err);
        throw err; // Reject the promise on error
    }
}
