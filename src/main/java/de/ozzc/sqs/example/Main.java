package de.ozzc.sqs.example;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Ozkan Can
 */
public class Main {

    public static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    public static final String QUEUE_NAME = "MyQueue";
    public static final Region REGION = Region.getRegion(Regions.EU_CENTRAL_1);

    public static void main(String[] args) throws InterruptedException, JMSException {

        // JMS
        SQSConnectionFactory connectionFactory =
                SQSConnectionFactory.builder()
                        .withRegion(REGION)
                        .withAWSCredentialsProvider(new DefaultAWSCredentialsProviderChain())
                        .build();
        SQSConnection connection = connectionFactory.createConnection();

        AmazonSQS sqs = connection.getAmazonSQSClient();
        sqs.setRegion(REGION);
        Map<String, String> queueAttributes = new HashMap<>();
        queueAttributes.put("VisibilityTimeout", "120");
        LOGGER.info("Creating Queue " + QUEUE_NAME + " with VisibilityTimeout of 120s.");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME).withAttributes(queueAttributes);
        String myQueueUrl;
        try {
            myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        } catch (QueueDeletedRecentlyException e) {
            LOGGER.info("Queue was deleted recently. Waiting for 65s before we continue ...");
            Thread.sleep(TimeUnit.SECONDS.toMillis(65));
            myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        }
        LOGGER.info(myQueueUrl);

        sqs.sendMessage(new SendMessageRequest(QUEUE_NAME, "Hello World!"));
        GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(new GetQueueAttributesRequest(myQueueUrl));
        if (getQueueAttributesResult != null) {
            Map<String, String> attributes = getQueueAttributesResult.getAttributes();
            if (attributes != null && !attributes.isEmpty()) {
                Set<String> keys = attributes.keySet();
                LOGGER.info("Queue Attributes for " + myQueueUrl);
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
        sqs.deleteQueue(QUEUE_NAME);
        ListQueuesResult listQueuesResult = sqs.listQueues(QUEUE_NAME);
        if (listQueuesResult != null) {
            if (listQueuesResult.getQueueUrls().size() > 0) {
                List<String> queueUrls = listQueuesResult.getQueueUrls();
                for (String queueUrl : queueUrls)
                    LOGGER.info("Existing Queue that was not deleted : " + queueUrl);
            }
        }


        try {
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
            if (!client.queueExists("TopicQueue")) {
                CreateQueueResult createQueueResult = client.createQueue("TopicQueue");
                LOGGER.info("TopicQueue created");
                sqs.deleteQueue(new DeleteQueueRequest("TopicQueue"));
            }
        } catch (JMSException e) {
            LOGGER.error(e.getMessage(), e);
        }
        finally {
            connection.close();
        }
    }
}
