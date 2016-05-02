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
 *
 * @author Ozkan Can
 */
public class Main {

    public static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    public static final String myQueue = "MyQueue";

    public static void main(String[] args) throws InterruptedException {
        AmazonSQS sqs = new AmazonSQSClient(new DefaultAWSCredentialsProviderChain());
        sqs.setRegion(Region.getRegion(Regions.EU_CENTRAL_1));
        ListQueuesResult listQueuesResult = sqs.listQueues(myQueue);
        if(listQueuesResult != null)
        {
            if(listQueuesResult.getQueueUrls().size() > 0)
            {
                LOGGER.info("Deleting queue MyQueue");
                sqs.deleteQueue(myQueue);
                LOGGER.info("Waiting for 65s before we continue ...");
                Thread.sleep(TimeUnit.SECONDS.toMillis(65));
            }
        }
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(myQueue);
        String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
        sqs.sendMessage(new SendMessageRequest(myQueue, "Hello World!"));
        ReceiveMessageResult receiveMessageResult = sqs.receiveMessage(myQueue);
        if(receiveMessageResult != null)
        {
            List<Message> messages = receiveMessageResult.getMessages();
            if(messages != null && messages.size() > 0)
            {
                for(Message message : messages)
                {
                    LOGGER.info(message.getBody());
                }
            }
        }
        LOGGER.info(myQueueUrl);
    }
}
