package com.test.springboot;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

@Slf4j
public class SimpleTest {


    @Test
    public void syncProducer() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        DefaultMQProducer producer = getProducer();

        IntStream.range(0, 10).forEach(index -> {
            Message message = new Message("simple_test", "Tag-1", ("hello rocketmq " + index).getBytes());
            try {
                SendResult result = producer.send(message);
                log.info(result.toString());
            } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        new CountDownLatch(1).await();
    }

    @Test
    public void asyncProducer() throws InterruptedException {
        DefaultMQProducer producer = getProducer();

        CountDownLatch latch = new CountDownLatch(101);


        IntStream.range(0, 100).forEach(index -> {
            Message message = new Message("simple_test", "Tag-1", ("hello rocketmq async " + index).getBytes());
            try {
                producer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        log.info("发送成功：{}", sendResult.toString());
                        latch.countDown();
                    }

                    @Override
                    public void onException(Throwable e) {
                        log.error("发送失败：", e);
                        latch.countDown();
                    }
                });
            } catch (MQClientException | RemotingException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        latch.await();
    }

    /**
     * 对可靠性要求不高 如日志记录
     * 不需要等待应答
     *
     * @throws InterruptedException
     */
    @Test
    public void OnewayProducer() throws InterruptedException {
        DefaultMQProducer producer = getProducer();

        IntStream.range(0, 100).forEach(index -> {
            Message message = new Message("simple_test", "Tag-1", ("hello rocketmq one way " + index).getBytes());
            try {
                producer.sendOneway(message);
            } catch (MQClientException | RemotingException | InterruptedException e) {
                e.printStackTrace();
            }

        });

        new CountDownLatch(1).await();
    }

    @SneakyThrows
    private DefaultMQProducer getProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("simple", true);
        producer.setNamesrvAddr("localhost:9876;localhost:9877;localhost:9878");
        producer.start();
        return producer;
    }


    @Test
    public void customer() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("simple", true);
        consumer.setNamesrvAddr("localhost:9876;localhost:9877;localhost:9878");
        consumer.subscribe("simple_test", "Tag-1 || Tag-2");
        consumer.setConsumeThreadMax(5);
        consumer.setConsumeThreadMin(5);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            log.info("reveive new messages :{}|{}", context, msgs);
            msgs.forEach(msg -> {
                log.debug(new String(msg.getBody()));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        new CountDownLatch(1).await();
    }

}
