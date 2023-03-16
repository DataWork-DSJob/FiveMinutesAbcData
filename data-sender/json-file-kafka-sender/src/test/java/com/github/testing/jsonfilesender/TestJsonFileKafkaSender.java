package com.github.testing.jsonfilesender;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/9/16 14:33
 */
public class TestJsonFileKafkaSender {

    /**
     * Eoi测试环境, 192.168.110.79:9092
     *  bdnode112:9092
     *
     *  bdnode124:9092
     */
    private String bootstrapServers = "bdnode124:9092";
    private String topic = "testSourceTopic";

    @Ignore
    @Test
    public void testSender(){
        JsonFileKafkaSender.main(new StringJoiner(" ")
                .add("--bootstrapServices").add(bootstrapServers) // bootstrapServer
                .add("--topic").add("testSinkTopic")
                .add("--reporterPeriodSeconds").add("10")
                .add("--recordsPerSecond").add("300")
                .add("--kafkaProps").add("compression.type=lz4")
//                .add("--enableNotLimitRate").add("true")
                .add("--jsonTemplateFile").add("D:\\githubsrc\\common-data-dev\\data-sender\\json-file-kafka-sender\\src\\main\\resources\\data.json")
                .toString().split(" "));

    }

    @Ignore
    @Test
    public void testConsumerSubscribeSucceed(){
        String groupId = this.getClass().getSimpleName();

        Properties props = new Properties();
        props.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put( ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        props.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singleton(topic));

        MetricRegistry registry = new MetricRegistry();
        Meter rateMeter = registry.meter("test_kafka_consumer_rate-" + groupId);
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
        reporter.start(5, TimeUnit.SECONDS);

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(500);
            if (null != poll) {
                try {
                    for(ConsumerRecord<String, String> record: poll) {
                        String key = record.key();
                        String value = record.value();
                        JSONObject json = JSON.parseObject(value);
                        rateMeter.mark();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        }

    }




}
