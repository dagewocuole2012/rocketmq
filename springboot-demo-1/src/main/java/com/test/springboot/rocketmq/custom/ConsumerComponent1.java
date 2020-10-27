package com.test.springboot.rocketmq.custom;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.springframework.stereotype.Component;

/**
 * @author wangke
 */
@Component
public class ConsumerComponent1 extends BaseConsumerComponent {

    @Override
    protected DefaultMQPushConsumer getMQPushConsumer() {
        return new DefaultMQPushConsumer("springboot-group-1");
    }

    @Override
    protected String getTags() {
        return "Tag-2 || Tag-3";
    }

    @Override
    protected String getTopic() {
        return "testmq";
    }
}