package com.test.springboot.rocketmq;

import com.test.springboot.rocketmq.event.MQMessageEvent;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author wangke
 */
public abstract class BaseConsumerComponent implements ApplicationContextAware {

    @Getter
    @Setter
    private DefaultMQPushConsumer consumer;

    @Getter
    private ApplicationContext applicationContext;

    @PostConstruct
    public void initMQConsumer() throws MQClientException {
        setConsumer(
                getMQPushConsumer()
        );

        getConsumer().setNamesrvAddr("localhost:9876");
        getConsumer().subscribe("testmq", "*");
        getConsumer().registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            msgs.parallelStream().forEach(messageExt -> {

                getApplicationContext().publishEvent(
                        new MQMessageEvent(messageExt.getBody())
                );

            });

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        getConsumer().start();

    }

    protected abstract DefaultMQPushConsumer getMQPushConsumer();

    @PreDestroy
    public void destroy() {
        getConsumer().shutdown();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
