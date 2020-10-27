package com.test.springboot.rocketmq;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author wangke
 */
@Component
public class ProducerComponent {

    @Setter
    @Getter
    private DefaultMQProducer producer;


    /**
     * 初始化mq
     *
     * @throws MQClientException
     */
    @PostConstruct
    public void initMQProducer() throws MQClientException {

        setProducer(
                new DefaultMQProducer("springboot-demo")
        );

        getProducer().setNamesrvAddr("localhost:9876");

        getProducer().start();

    }

    /**
     * 发送消息
     *
     * @param topic
     * @param tag
     * @param content
     * @return
     */
    public boolean send(String topic, String tag, String content) {
        Message message = new Message(topic, tag, content.getBytes());
        try {
            SendResult result = getProducer().send(message);
            if (result.getSendStatus().equals(SendStatus.SEND_OK)) {
                return true;
            }
        } catch (MQClientException | InterruptedException | MQBrokerException | RemotingException e) {
            e.printStackTrace();
        }
        return false;
    }

    @PreDestroy
    public void destory() {
        getProducer().shutdown();
    }
}
