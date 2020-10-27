package com.test.springboot.rocketmq;

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
}
