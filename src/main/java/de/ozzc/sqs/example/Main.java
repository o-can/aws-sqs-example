package de.ozzc.sqs.example;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Ozkan Can
 */
public class Main {

    public static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    public static final String myQueue = "MyQueue";

    public static void main(String[] args) throws InterruptedException {
        AmazonSQS sqs = new AmazonSQSClient(new DefaultAWSCredentialsProviderChain());
        sqs.setRegion(Region.getRegion(Regions.EU_CENTRAL_1));
        Map<String, String> queueAttributes = new HashMap<>();
        queueAttributes.put("VisibilityTimeout", "120");
        LOGGER.info("Creating Queue "+myQueue+" with VisibilityTimeout of 120s.");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(myQueue).withAttributes(queueAttributes);
        String myQueueUrl;
        try {
            myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        } catch (QueueDeletedRecentlyException e) {
            LOGGER.info("Queue was deleted recently. Waiting for 65s before we continue ...");
            Thread.sleep(TimeUnit.SECONDS.toMillis(65));
            myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        }
        LOGGER.info(myQueueUrl);

        sqs.sendMessage(new SendMessageRequest(myQueue, "Hello World!"));
        GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(new GetQueueAttributesRequest(myQueueUrl));
        if(getQueueAttributesResult != null)
        {
            Map<String, String> attributes = getQueueAttributesResult.getAttributes();
            if(attributes != null && !attributes.isEmpty())
            {
                Set<String> keys = attributes.keySet();
                LOGGER.info("Queue Attributes for "+myQueueUrl);
            }
        }
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(myQueueUrl).withMaxNumberOfMessages(1);
        boolean messageReceived = false;
        while (!messageReceived) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
            if (receiveMessageResult != null) {
                List<Message> messages = receiveMessageResult.getMessages();
                if (messages != null && messages.size() == 1) {
                    messageReceived = true;
                    Message message = messages.get(0);
                    LOGGER.info("Changing message visability to 60s.");
                    sqs.changeMessageVisibility(myQueueUrl, message.getReceiptHandle(), 60);
                    LOGGER.info("Received message : " + message.getBody());
                    sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(myQueueUrl).withReceiptHandle(message.getReceiptHandle()));
                }
            }
        }
        sqs.purgeQueue(new PurgeQueueRequest().withQueueUrl(myQueueUrl));
        sqs.deleteQueue(myQueue);
        ListQueuesResult listQueuesResult = sqs.listQueues(myQueue);
        if (listQueuesResult != null) {
            if (listQueuesResult.getQueueUrls().size() > 0) {
                List<String> queueUrls = listQueuesResult.getQueueUrls();
                for (String queueUrl : queueUrls)
                    LOGGER.info("Existing Queue that was not deleted : " + queueUrl);
            }
        }
    }
}
