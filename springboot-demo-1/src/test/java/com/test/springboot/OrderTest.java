package com.test.springboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * 顺序消费
 * 一个topic对应多个队列。
 * 『一个』队列同一时刻只能被『一个』消费者中的『一个』线程消费。
 */
@Slf4j
public class OrderTest {

    @Test
    public void producer() throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("order_test");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                Message message = new Message("test_order", "test", ("order-1-" + i).getBytes());
                try {
                    SendResult result = producer.send(message, (mqs, msg, arg) -> {
                        int index = (int) arg % mqs.size();
                        return mqs.get(index);
                    }, 1);
                    log.debug(result.getSendStatus().name());
                } catch (MQClientException | RemotingException | InterruptedException | MQBrokerException e) {
                    e.printStackTrace();
                }
            }
        }, "order1").start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                Message message = new Message("test_order", "test", ("order-2-" + i).getBytes());
                try {
                    SendResult result = producer.send(message, (mqs, msg, arg) -> {
                        int index = (int) arg % mqs.size();
                        return mqs.get(index);
                    }, 2);
                    log.debug(result.getSendStatus().name());
                } catch (MQClientException | RemotingException | InterruptedException | MQBrokerException e) {
                    e.printStackTrace();
                }
            }
        }, "order2").start();

        new CountDownLatch(1).await();

    }

    @Test
    public void consumer() throws MQClientException, InterruptedException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_test");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("test_order", "*");

        consumer.setConsumeThreadMin(3);//即使使用多线程消费端

        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {

            msgs.forEach(msg ->
                    log.info("{} received message :{}", Thread.currentThread().getName(), new String(msg.getBody()))
            );


            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();

        new CountDownLatch(1).await();

    }

}
