package com.test.springboot.rocketmq.event;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEvent;

/**
 * @author wangke
 */
@Slf4j
public class MQMessageEvent extends ApplicationEvent {


    @Getter
    @Setter
    private byte[] messageBytes;

    /**
     * Create a new {@code ApplicationEvent}.
     *
     * @param source the object on which the event initially occurred or with
     *               which the event is associated (never {@code null})
     */
    public MQMessageEvent(Object source) {
        super(source);
        setMessageBytes((byte[]) source);
    }

    @Override
    public String toString() {
        return new String(getMessageBytes());
    }
}
