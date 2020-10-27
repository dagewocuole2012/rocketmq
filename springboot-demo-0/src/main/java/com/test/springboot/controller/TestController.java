package com.test.springboot.controller;

import com.test.springboot.rocketmq.ProducerComponent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wangke
 */
@RestController
@Slf4j
public class TestController {

    @Autowired
    ProducerComponent producerComponent;

    @GetMapping("/test/{arg}")
    public String test(@PathVariable String arg) {
        log.info("received:" + arg);
        return "hello " + arg;
    }

    @GetMapping("/testmq/{content}")
    public boolean testmq(@PathVariable String content) {
        log.info("received message: " + content);
        return producerComponent.send("testmq", "Tag-" + content, content);
    }

}
