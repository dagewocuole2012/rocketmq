package com.test.springboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事务消息
 */
@Slf4j
public class TransactionTest {

    @Test
    public void consumer() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_test");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("test_transaction", "*");
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
    public void provider() throws MQClientException, InterruptedException {
        TransactionMQProducer producer = new TransactionMQProducer("transaction_test");
        AtomicInteger atomicInteger = new AtomicInteger();
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(2, 10, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(100), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("transaction_check_thread_" + atomicInteger.incrementAndGet());
                return thread;
            }
        }, new ThreadPoolExecutor.AbortPolicy());
        producer.setExecutorService(new ForkJoinPool());
        producer.setTransactionListener(new TransactionListenerImpl());
        producer.setNamesrvAddr("localhost:9876");

        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message("test_transaction", "tag-1", ("order---" + i).getBytes());
            log.debug("完成订单{}的组装。", i);
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, i);
            log.info(sendResult.getSendStatus().toString());
            log.info(sendResult.getLocalTransactionState().toString());
            TimeUnit.SECONDS.sleep(5);
        }

        new CountDownLatch(1).await();

    }

    @Test
    public void random() {
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            log.info(random.nextInt(10) + "");
        }
    }
}


@Slf4j
class TransactionListenerImpl implements TransactionListener {
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {

        //本地事务提交 与业务发送代码，在同一个事务「线程」中执行
        //先执行，再返回发送状态
        log.debug("{}本地事务执行,arg「{}」", new String(msg.getBody()), arg);
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Random random = new Random();
        int i = random.nextInt(10);
        log.info("{}本地事务执行结果：{}", new String(msg.getBody()), i);
        try {
            TimeUnit.SECONDS.sleep(i);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (i >= 9) return LocalTransactionState.ROLLBACK_MESSAGE;//十分之一的概率回滚
        if (i <= 3) return LocalTransactionState.COMMIT_MESSAGE;//十分之四的概率提交
        return LocalTransactionState.UNKNOW;//其他继续等待
    }
}
