package com.github.testing.jsonfilesender;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @projectName: common-data-dev
 * @className: KafkaSenderRunnable
 * @description: com.github.testing.jsonfilesender.KafkaSenderRunnable
 * @author: jiaqing.he
 * @date: 2022/12/9 17:45
 * @version: 1.0
 */
public class KafkaSenderRunnable implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSenderRunnable.class);

    private Argument argument;
    private Callable<JSONObject> strRecordCreator;
    private int limitSecondRate;
    private int runnerCount;

    private String topic;
    private KafkaProducer<String, String> producer;
    private boolean isRunning;

    public KafkaSenderRunnable(Argument argument, Callable<JSONObject> strRecordCreator,
                               int limitSecondRate, int runnerCount) {
        this.argument = argument;
        this.strRecordCreator = strRecordCreator;
        this.limitSecondRate = limitSecondRate;
        this.runnerCount = runnerCount;
    }

    public void init(Argument argument) {
        this.topic = argument.getTopic();
        String bootstrapServers = argument.getBootstrapServices();
        Map<String, String> kafkaPropsConf = argument.getKafkaProps();
        Properties kafkaProps = new Properties();
        if (null != kafkaPropsConf) {
            kafkaProps.putAll(kafkaPropsConf);
        }
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 优化写入性能
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "0");
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384 * 2);
//        kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        this.producer = new KafkaProducer<String, String>(kafkaProps);
        this.isRunning = true;
    }

    @Override
    public void run() {
        init(argument);
        String tid = "tid-" + Thread.currentThread().getId();
        MetricRegistry registry = new MetricRegistry();
        Meter rateMeter = registry.meter("KafkaSenderRunnable_runner-" + runnerCount + "_" + tid);
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();
        reporter.start(argument.getReporterPeriodSeconds(), TimeUnit.SECONDS);

        boolean enableNotLimit = argument.isEnableNotLimitRate();
        if (enableNotLimit) {
            while (isRunning) {
                try {
                    JSONObject json = strRecordCreator.call();
                    String str = json.toJSONString();
                    producer.send(new ProducerRecord(topic, null, str));
                    rateMeter.mark();
                } catch (Exception e) {
                    LOG.warn("CreateOrSend once Error", e);
                }
            }
        } else {
            RateLimiter rateLimiter = RateLimiter.create(limitSecondRate * 1.001);
            while (isRunning) {
                try {
                    JSONObject json = strRecordCreator.call();
                    String str = json.toJSONString();
                    producer.send(new ProducerRecord(topic, null, str));
                    rateMeter.mark();
                    rateLimiter.acquire();
                } catch (Exception e) {
                    LOG.warn("CreateOrSend once Error", e);
                }
            }
        }

    }


}
