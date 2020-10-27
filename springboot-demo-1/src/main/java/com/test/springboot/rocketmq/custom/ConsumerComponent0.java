package com.test.springboot.rocketmq.custom;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.springframework.stereotype.Component;

/**
 * @author wangke
 */
@Component
public class ConsumerComponent0 extends BaseConsumerComponent {

    @Override
    protected DefaultMQPushConsumer getMQPushConsumer() {
        return new DefaultMQPushConsumer("springboot-group-0");
    }

    @Override
    protected String getTags() {
        return "Tag-1 || Tag-2";
    }

    @Override
    protected String getTopic() {
        return "testmq";
    }
}
