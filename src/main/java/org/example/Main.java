package org.example;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class Main {
    private static final String TOPIC4 = "TopicTest";
    private static final String TOPIC5 = "TopicTest5";
    private static final String TAG = "tag";
    private static final String DEFAULT_NAMESRVADDR = "tiger.rocketmq.com:8876";
    private static final String DEFAULT_PROXY = "tiger.rocketmq.com:8001;tiger.rocketmq.com:8002";
    private static final String CONSUMER_GROUP = "GroupTest";
    private static final String CONSUMER_GROUP5 = "GroupTest";

    public static void main(String[] args) throws MQClientException, InterruptedException, ClientException, IOException {
        consumer4();
        consumer5();
        produce4();
        produce5();

        Thread.sleep(Long.MAX_VALUE);
        System.out.println("Hello world!");
    }

    private static void produce5() throws ClientException, InterruptedException {
        new Thread(()->{
            ClientServiceProvider provider = ClientServiceProvider.loadService();

            String accessKey = "";
            String secretKey = "";
            SessionCredentialsProvider sessionCredentialsProvider =
                    new StaticSessionCredentialsProvider(accessKey, secretKey);

            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(DEFAULT_PROXY)
                    .enableSsl(false)
                    .setCredentialProvider(sessionCredentialsProvider)
                    .build();
            final Producer producer;
            try {
                producer = provider.newProducerBuilder()
                        .setClientConfiguration(clientConfiguration)
                        .setTopics(TOPIC5)
                        .build();
            } catch (ClientException e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < 128; i++) {
                byte[] body = ("This is a FIFO message for Apache RocketMQ 中文中文" + i).getBytes(StandardCharsets.UTF_8);
                String tag = "yourMessageTagA" + i;
                final org.apache.rocketmq.client.apis.message.Message message = provider.newMessageBuilder()
                        .setTopic(TOPIC5)
                        .setTag(tag)
                        .setBody(body)
                        .build();

                try {
                    final SendReceipt sendReceipt = producer.send(message);
                    System.out.println("produce 5 :" + sendReceipt.getMessageId().toString());
                } catch (Throwable t) {
                    t.printStackTrace();
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            System.out.println("produce 5 done");
        }).start();

    }

    public static void produce4() throws MQClientException, InterruptedException {
        new Thread(()->{
            DefaultMQProducer producer = new DefaultMQProducer("pppp");
            producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

            try {
                producer.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < 128; i++) {
                try {
                    Message msg = new Message(TOPIC4, TAG, "OrderID188", "Hello world".getBytes(StandardCharsets.UTF_8));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("produce 4 %s%n", sendResult);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            producer.shutdown();
            System.out.println("produce4 done");
        }).start();
    }

    public static void consumer4() throws MQClientException {
        new Thread(() -> {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
            consumer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
            try {
                consumer.subscribe(TOPIC4, "*");
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    System.out.printf("consume 4 %s %n", msgs);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            try {
                consumer.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        }).start();
        System.out.printf("consumer 4 started.%n");
    }

    public static void consumer5() throws ClientException, InterruptedException, IOException {
        new Thread(() -> {
            final ClientServiceProvider provider = ClientServiceProvider.loadService();

            String accessKey = "";
            String secretKey = "";
            SessionCredentialsProvider sessionCredentialsProvider =
                    new StaticSessionCredentialsProvider(accessKey, secretKey);
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(DEFAULT_PROXY)
                    .setCredentialProvider(sessionCredentialsProvider)
                    .enableSsl(false)
                    .build();
            String tag = "*";
            FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
            try {
                Charset charset = Charset.forName("UTF-8"); // 假设你的数据是使用UTF-8编码的

                PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                        .setClientConfiguration(clientConfiguration)
                        .setConsumerGroup(CONSUMER_GROUP5)
                        .setSubscriptionExpressions(Collections.singletonMap(TOPIC5, filterExpression))
                        .setMessageListener(messageView -> {
                            System.out.println("consume 5: " + charset.decode(messageView.getBody()));
                            return ConsumeResult.SUCCESS;
                        })
                        .build();

            } catch (ClientException e) {
                throw new RuntimeException(e);
            }
            System.out.println("consumer 5 started");
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();


    }
}