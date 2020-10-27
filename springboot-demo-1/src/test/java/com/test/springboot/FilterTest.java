package com.test.springboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
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
 * 基于tag和properties的过滤器
 * <p>
 * properties过滤要开启broker的配置enablePropertyFilter=true
 * http://rocketmq.apache.org/docs/filter-by-sql92-example/
 */
@Slf4j
public class FilterTest {

    @Test
    public void producer() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("filter_test");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        Message message = new Message("test_filter", "tag_1", "hello".getBytes());
        message.putUserProperty("customKey", "3");

        SendResult send = producer.send(message);
        log.info(send.getSendStatus().toString());
    }

    @Test
    public void customer_properties() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("filter_test");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("test_filter", MessageSelector.bySql("customKey = '3'"));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            StringJoiner stringJoiner = new StringJoiner("\n", "「", "」");
            msgs.forEach(msg -> stringJoiner.add(new String(msg.getBody())));

            log.info("接收到{}条消息：\n {}\n v=====================================================",
                    msgs.size(),
                    stringJoiner.toString()
            );

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        new CountDownLatch(1).await();
    }
}
