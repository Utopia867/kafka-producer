package com.example.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author wangwentao
 * @date 2020/7/29 15:19
 */
public class KafkaSender {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 发送消息到kafka
     *
     * @param topic   主题
     * @param message 内容体
     */
    public void sendMsg(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }
}
