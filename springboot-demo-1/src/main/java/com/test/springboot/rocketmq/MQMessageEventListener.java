package com.test.springboot.rocketmq;

import com.test.springboot.rocketmq.event.MQMessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * @author wangke
 */
@Component
@Slf4j
public class MQMessageEventListener implements ApplicationListener<MQMessageEvent> {


    @Override
    public void onApplicationEvent(MQMessageEvent event) {

        log.info(event.toString());
    }
}
