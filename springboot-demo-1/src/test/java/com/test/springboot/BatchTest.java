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

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 批量消息
 */
@Slf4j
public class BatchTest {

    @Test
    public void consumer() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("batch_test");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("test_batch", "*");
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
        DefaultMQProducer producer = new DefaultMQProducer("barch_test");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        //注意这里1w的时候还可以。
        //但是100w的时候，就会报「MQClientException: CODE: 13  DESC: the message body size over max value，MAX: 4194304」
        //最大4M
        int count = 1000000;
        List<Message> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Message message = new Message("test_batch", "tag", ("dbody-" + i).getBytes());
            messages.add(message);
        }

        //为了每次发送都在4Mb以下。
        //官网提供ListSplitter进行数组拆分的demo {@link SplitBatchProducer}
        //①常规发送
//        SendResult send = producer.send(messages);
//        log.info(send.getSendStatus().toString());


        //②拆分发送
        ListSplitter splitter = new ListSplitter(messages);

        while (splitter.hasNext()) {
            SendResult sendResult = producer.send(splitter.next());
            log.info(sendResult.getSendStatus().toString());
        }
        log.info("发送完毕");

    }


}


/**
 * 官网demo SplitBatchProducer
 */
class ListSplitter implements Iterator<List<Message>> {
    //这里是数据包大小「包括属性和消息体」，而不是条数
    //1M
    //TODO 这里还要看一下批量发送的源码 因为设置为4M依旧报错
    private int sizeLimit = 1024 * 1024 ;
    private final List<Message> messages;
    private int currIndex;

    public ListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currIndex;
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            int tmpSize = message.getTopic().length() + message.getBody().length;
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            tmpSize = tmpSize + 20; //for log overhead
            if (tmpSize > sizeLimit) {
                //it is unexpected that single message exceeds the sizeLimit
                //here just let it go, otherwise it will block the splitting process
                if (nextIndex - currIndex == 0) {
                    //if the next sublist has no element, add this one and then break, otherwise just break
                    nextIndex++;
                }
                break;
            }
            if (tmpSize + totalSize > sizeLimit) {
                break;
            } else {
                totalSize += tmpSize;
            }

        }
        List<Message> subList = messages.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not allowed to remove");
    }
}
