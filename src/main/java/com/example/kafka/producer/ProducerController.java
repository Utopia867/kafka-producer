package com.example.kafka.producer;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author wangwentao
 * @date 2020/7/28 21:37
 */
@Log4j2
@RestController
public class ProducerController {

    public static final String brokerList = "172.20.6.23:9092,172.20.6.21:9092,172.20.6.24:9093";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        return properties;
    }

    @RequestMapping(value = "/send", method = {RequestMethod.POST})
    public void sentMsg(HttpServletRequest request) {
        String msg = this.getPostData(request);
        log.info("发送消息：{}", msg);

        KafkaProducer<String, String> producer = new KafkaProducer<>(initConfig());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        try {
            producer.send(record);
//            Future<RecordMetadata> future = producer.send(record);
//            RecordMetadata recordMetadata = future.get();
//            log.info("recordMetadata->{}", recordMetadata.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取POST参数
     *
     * @param request
     * @return
     */
    public String getPostData(HttpServletRequest request) {
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream(), "utf-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            log.error("读取请求参数失败", e);
        } finally {
            try {
                request.getInputStream().close();
            } catch (IOException e) {
                log.error("关闭流失败", e);
            }
        }
        return sb.toString();
    }


}
