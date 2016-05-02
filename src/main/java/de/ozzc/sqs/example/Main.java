package de.ozzc.sqs.example;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Ozkan Can
 */
public class Main {

    public static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    public static final String myQueue = "MyQueue";

    public static void main(String[] args) throws InterruptedException {
        AmazonSQS sqs = new AmazonSQSClient(new DefaultAWSCredentialsProviderChain());
        sqs.setRegion(Region.getRegion(Regions.EU_CENTRAL_1));
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(myQueue);
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
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(myQueueUrl).withMaxNumberOfMessages(1);
        boolean messageReceived = false;
        while (!messageReceived) {
            ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(receiveMessageRequest);
            if (receiveMessageResult != null) {
                List<Message> messages = receiveMessageResult.getMessages();
                if (messages != null && messages.size() == 1) {
                    messageReceived = true;
                    Message message = messages.get(0);
                    LOGGER.info("Received message : " + message.getBody());
                    sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(myQueueUrl).withReceiptHandle(message.getReceiptHandle()));
                }
            }
        }
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
