package com.javatechie.aws.sqs;

import com.amazonaws.services.sqs.model.*;
import com.javatechie.aws.sqs.config.AwsSQSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration;
import org.springframework.cloud.aws.messaging.core.QueueMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@SpringBootApplication(exclude = {ContextStackAutoConfiguration.class})
@RestController
public class SpringbootAwsSqsExeApplication {


    Logger logger= LoggerFactory.getLogger(SpringbootAwsSqsExeApplication.class);

    @Autowired
    private QueueMessagingTemplate queueMessagingTemplate;

    @Autowired
    AwsSQSConfig sqs;

    @Value("${cloud.aws.end-point.uri}")
    private String endpoint;

    @GetMapping("/send/{message}")
    public void sendMessageToQueue(@PathVariable String message) throws InterruptedException {

        for (int i = 0; i < 50; i++) {

            String xx = UUID.randomUUID().toString();
            String deDup = xx + i;
            String msg = xx + " " + i;

            SendMessageRequest sendMessageFifoQueue = new SendMessageRequest().withQueueUrl(endpoint).withMessageBody(
                    msg).withMessageDeduplicationId(deDup).withMessageGroupId("my-group-1");

            sqs.amazonSQSAsync().sendMessage(sendMessageFifoQueue);

            Thread.sleep(400);
        }
    }

    @GetMapping("/receive")
    public void loadMessageFromSQS()  {

        GetQueueAttributesRequest gs = new GetQueueAttributesRequest(endpoint).withAttributeNames("ApproximateNumberOfMessages");

        Map<String, String> xx = sqs.amazonSQSAsync().getQueueAttributes(gs).getAttributes();

        int count = Integer.parseInt(xx.get("ApproximateNumberOfMessages"));

        while (count > 0) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(endpoint).withWaitTimeSeconds(10).withMaxNumberOfMessages(10);

            List<Message> sqsMessages = sqs.amazonSQSAsync().receiveMessage(receiveMessageRequest).getMessages();

            for (Message m : sqsMessages) {
                logger.info("message from SQS Queue {}", m.getBody());
                sqs.amazonSQSAsync().deleteMessage(new DeleteMessageRequest().withQueueUrl(endpoint).withReceiptHandle(m.getReceiptHandle()));
            }

           count = Integer.parseInt(sqs.amazonSQSAsync().getQueueAttributes(gs).getAttributes().get("ApproximateNumberOfMessages"));
        }

    }

    @GetMapping("/count")
    public void count() {

        GetQueueAttributesRequest gs = new GetQueueAttributesRequest(endpoint).withAttributeNames("ApproximateNumberOfMessages");

        Map<String, String> xx = sqs.amazonSQSAsync().getQueueAttributes(gs).getAttributes();

        logger.info(xx.get("ApproximateNumberOfMessages"));
    }


    public static void main(String[] args) {
        SpringApplication.run(SpringbootAwsSqsExeApplication.class, args);
    }

}
