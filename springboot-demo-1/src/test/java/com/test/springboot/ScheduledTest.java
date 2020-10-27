package com.test.springboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;

/**
 * 定时消息
 * 消息放入延迟队列。从延迟队列定时移入消费队列
 * TODO 但是这个延时时间不是 实际消费时间-放入队列时间。
 */
@Slf4j
public class ScheduledTest {

    @Test
    public void consumer() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("schedule_test");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("test_schedule", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            StringJoiner stringJoiner = new StringJoiner("\n", "「", "」");
            msgs.forEach(msg -> stringJoiner.add(new String(msg.getBody())));

            log.info("接收到{}条消息：\n {}\n =====================================================",
                    msgs.size(),
                    stringJoiner.toString()
            );

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();

        new CountDownLatch(1).await();
    }

    @Test
    public void producer() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("schedule_tet");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        Message message = new Message("test_schedule", "tag-1", "hello schedule message".getBytes());

        //可以从delayLevelTable看到18个延迟等级
        //1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        message.setDelayTimeLevel(2);//延迟5秒

        SendResult result = producer.send(message);
        log.info(result.getSendStatus().name());
    }


}
