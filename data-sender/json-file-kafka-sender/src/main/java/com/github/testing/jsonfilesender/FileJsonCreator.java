package com.github.testing.jsonfilesender;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.testing.jsonfilesender.utils.FileIOHelper;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @projectName: common-data-dev
 * @className: FileJsonCreator
 * @description: com.github.testing.jsonfilesender.FileJsonCreator
 * @author: jiaqing.he
 * @date: 2022/12/9 18:27
 * @version: 1.0
 */
public class FileJsonCreator implements Callable<JSONObject> {

    public static final String JSON_TEMPLATE = "{\n" +
            "\t\"timestamp\": 1669166469000,\n" +
            "\t\"promInfo\": \"io_in_rate{devType=FireWall,instance=hadoop01,metricName=IO流入速率,businessName=," +
            "timestamp=2022-12-08 11:07:57,datasource=统一采集," +
            "addInfo={\\\"collect_center\\\":\\\"Shanghai_Eoi\\\"}} 3.1415\\n\",\n" +
            "\t\"jsonArrayStr\": \"[{\\\"metric\\\":\\\"cpu_second\\\",\\\"prometheus\\\":\\\"cpu_second" +
            "{instance=\\\\\\\"hadoop01\\\\\\\", app_id=\\\\\\\"app_202211260928\\\\\\\"} 2580.21\\\",\\\"val\\\":2580.21}," +
            "{\\\"metric\\\":\\\"heap_memory\\\",\\\"prometheus\\\":\\\"heap_memory{instance=\\\\\\\"kafka6\\\\\\\", " +
            "app_id=\\\\\\\"app_20221123933\\\\\\\"} 10485760\\\",\\\"val\\\":10485760}]\",\n" +
            "\t\"logHeader\": \"1669166386000|320|1|ZGBX|node1002\"\n" +
            "}\n";


    private JSONObject templateRecord;
    private Map.Entry<String, Object> timeEntry;

    public void init(Argument argument) {
        File jsonTemplateFile = argument.getJsonTemplateFile();
        JSONObject jsonTemplate;
        if (null != jsonTemplateFile && jsonTemplateFile.exists()) {
            jsonTemplate = FileIOHelper.readPointFileAsBean(jsonTemplateFile);
        } else {
            jsonTemplate = JSON.parseObject(JSON_TEMPLATE);
        }

        Map<String, String> kvTemplate = argument.getKvTemplate();
        if (null != kvTemplate && !kvTemplate.isEmpty()) {
            jsonTemplate.putAll(kvTemplate);
        }
        this.templateRecord = jsonTemplate;
        String timeField = null != argument.getTimeField() ? argument.getTimeField() : "timestamp";
        this.templateRecord.put(timeField, System.currentTimeMillis());

        Map.Entry<String, Object> targetEntity = null;
        for (Map.Entry<String, Object> entry: this.templateRecord.entrySet()) {
            if (timeField.equals(entry.getKey())) {
                targetEntity = entry;
            }
        }
        this.timeEntry = targetEntity;

    }


    @Override
    public JSONObject call() throws Exception {
        this.timeEntry.setValue(System.currentTimeMillis());
        return templateRecord;
    }
}
