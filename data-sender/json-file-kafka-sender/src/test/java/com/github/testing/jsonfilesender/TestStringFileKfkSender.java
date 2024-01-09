package com.github.testing.jsonfilesender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestStringFileKfkSender
 * @description: com.github.testing.jsonfilesender.TestStringFileKfkSender
 * @author: jiaqing.he
 * @date: 2023/3/23 10:57
 * @version: 1.0
 */
@Ignore
public class TestStringFileKfkSender {

    @Test
    public void testReadFileLineAndSendKafka() throws IOException {
        String filePath = "D:\\githubsrc\\allenhe888\\FiveMinutesAbcData\\data-sender\\json-file-kafka-sender\\data-to-send\\trade_window.txt";
        String bootstrapServers = "192.168.51.124:9092";
        String topic = "ods_trade_csv";
        int sleepMillis = 3000;

        List<String> lines = Files.readAllLines(Paths.get(filePath));

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);


        for (String line: lines) {
            if (null != line && !line.trim().isEmpty()) {
                producer.send(new ProducerRecord(topic, line));
                producer.flush();
                System.out.println("已发送数据: " + line);
            }
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.flush();
        producer.close();


    }
}
