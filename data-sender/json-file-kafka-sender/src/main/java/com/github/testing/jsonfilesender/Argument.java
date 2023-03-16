package com.github.testing.jsonfilesender;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import lombok.Data;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/9/16 15:47
 */
@Data
public class Argument {


    @Parameter(names = {"--help", "-h"}, help = true)
    private String help;


    @Parameter(names = {"--bootstrapServices"})
    private String bootstrapServices = "localhost:9092";

    @Parameter(names = {"--topic"})
    private String topic = "testSourceTopic";

    @DynamicParameter(names = {"--kafkaProps"})
    private Map<String, String> kafkaProps = new HashMap();

    @Parameter(names = {"--recordsPerSecond"})
    private int recordsPerSecond = 100;

    @Parameter(names = {"--enableNotLimitRate"}, arity = 1)
    private boolean enableNotLimitRate = false;

    @Parameter(names = {"--maxRatePerThread"}, description = "单线程最大速率, 超过此值则新启线程发kafka, 默认5万 单个线程")
    private int maxRatePerThread = 50000;

    @Parameter(names = "--reporterPeriodSeconds")
    private int reporterPeriodSeconds = 60;

    @Parameter(names = {"--jsonTemplateFile"}, converter = FileConvert.class)
    private File jsonTemplateFile;

    @DynamicParameter(names = {"--kvTemplate", "-kv"})
    private Map<String, String> kvTemplate = new HashMap();

    @Parameter(names = "--timeField")
    private String timeField = "timestamp";

}



