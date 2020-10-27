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
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 广播模式
 * TODO 广播模式有个不同就是消息会一直保存 比方发送了3条消息，不管重启多少次consumer，都会重复消费这三条消息
 * 而且。看不到消息积压情况。FIXME 这是为啥呢。
 */
@Slf4j
public class BroadcastTest {

    @Test
    public void producer() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("broadcast_test");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        Message message = new Message("test_broadcast", "test","hello everyone".getBytes()); //注意tag不能不填 不填默认""
        SendResult result = producer.send(message);
        log.info(result.toString());

        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void consumer0() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast_test");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("test_broadcast", "*");

        consumer.setMessageModel(MessageModel.BROADCASTING);//设置广播

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            for (MessageExt msg : msgs) {
                log.info("received message :{}", new String(msg.getBody()));
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        new CountDownLatch(1).await();
    }

    @Test
    public void consumer1() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast_test");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("test_broadcast", "*");

        consumer.setMessageModel(MessageModel.BROADCASTING);//设置广播

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            for (MessageExt msg : msgs) {
                log.info("received message :{}", new String(msg.getBody()));
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        new CountDownLatch(1).await();
    }
}
