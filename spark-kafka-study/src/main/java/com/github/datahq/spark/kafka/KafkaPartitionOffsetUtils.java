package com.github.datahq.spark.kafka;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class KafkaPartitionOffsetUtils {

    public static class PartitionOffsetRange{
        String topic;
        int partition;
        long fromOffset;
        long untilOffset;
        long delta;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getFromOffset() {
            return fromOffset;
        }

        public void setFromOffset(long fromOffset) {
            this.fromOffset = fromOffset;
        }

        public long getUntilOffset() {
            return untilOffset;
        }

        public void setUntilOffset(long untilOffset) {
            this.untilOffset = untilOffset;
        }


        public String getFromToUntilOffsetStr(){
            return fromOffset+" -> "+untilOffset+" = "+getDelta();
        }

        public long getDelta() {
            return delta;
        }

        public void calculateDelta() {
            this.delta = this.untilOffset - this.fromOffset;
        }
    }

    public static Map<String, List<PartitionOffsetRange>> parseOffsetRangeFromFilePath(String filePath) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line = null;
        ArrayList<String> lines = new ArrayList<>();
        while ((line=in.readLine())!=null){
            if(!line.isEmpty()){
                lines.add(line);
            }
        }
        in.close();
        return parseOffsetRangeFromStrLines(lines);
    }

    public static Map<String, List<PartitionOffsetRange>> parseStringAsOffsetRange(String str){
        String[] lineArray = str.split("\n");
        return parseOffsetRangeFromStrLines(Arrays.asList(lineArray));
    }

    private static Map<String, List<PartitionOffsetRange>> parseOffsetRangeFromStrLines(List<String> lines){
        Map<String, List<PartitionOffsetRange>> topicMap = new HashMap<>();
        for (String line: lines) {
            String[] eles = line.split("    ");
            if(eles.length==3){
                PartitionOffsetRange offsetRange = new PartitionOffsetRange();
                for(String entry:eles){
                    String[] kv = entry.trim().split(":");
                    parseKafkaPartitionOffset(kv[0],kv[1],offsetRange);
                }
                String topic = offsetRange.getTopic();

                List<PartitionOffsetRange> list = topicMap.computeIfAbsent(topic,k->new ArrayList<>());
                list.add(offsetRange);
            }
        }
        return topicMap;
    }

    public static void printOffsetRange(Map<String, List<PartitionOffsetRange>> topicMap){

        topicMap.forEach((topic,list)->{
            System.out.println(topic);
            long deltaSum = 0L;
            long minValue = Long.MAX_VALUE;
            long maxValue = Long.MIN_VALUE;
            TreeMap<Long, PartitionOffsetRange> tree = new TreeMap<>();
            for(PartitionOffsetRange partition:list){
                System.out.println("\t"+partition.partition +"-> "+ partition.getFromToUntilOffsetStr());
                long delta = partition.getDelta();
                deltaSum += delta;
                if(delta < minValue){
                    minValue = delta;
                }
                if(delta > maxValue){
                    maxValue = delta;
                }
                tree.put(delta,partition);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {}
            int count = list.size();
            if(count>0){
                long averageValue = deltaSum / count;
                System.err.println(topic+" 统计: count="+count+", minDelta="+minValue+", maxDelta="+maxValue+", averageDelta="+averageValue);
                if(tree.size()>3){
                    for (int i = 0; i < 3; i++) {
                        Map.Entry<Long, PartitionOffsetRange> ele = tree.pollFirstEntry();
                        System.err.println("\t最小Delta "+ele.getKey()+" : "+ele.getValue().getFromToUntilOffsetStr());
                    }

                }
            }



        });
    }


    @Test
    public void testKafkaPartitionMetadataParse(){
        String lines = "" +
                "topic: MEASURE_POINT_INTERNAL_o15957475685781    partition: 3    offsets: 2681068463 to 2681087213\n" +
                "topic: MEASURE_POINT_INTERNAL_o15957475685781    partition: 19    offsets: 1283781919 to 1283800669\n" +
                "topic: MEASURE_POINT_INTERNAL_o15957475685781    partition: 4    offsets: 2166322545 to 2166341295";
        Map<String, List<PartitionOffsetRange>> stringListMap = KafkaPartitionOffsetUtils.parseStringAsOffsetRange(lines);
        KafkaPartitionOffsetUtils.printOffsetRange(stringListMap);
    }

    private static void parseKafkaPartitionOffset(String key, String valueStr,PartitionOffsetRange offsetRange) {
        String name = key.trim().toLowerCase();
        switch (name){
            case "topic":
                offsetRange.setTopic(valueStr.trim());
                break;
            case "partition":
                offsetRange.setPartition(Integer.parseInt(valueStr.trim()));
                break;
            case "offsets":
                String[] fromUntil = valueStr.trim().split("to");
                offsetRange.setFromOffset(Long.parseLong(fromUntil[0].trim()));
                offsetRange.setUntilOffset(Long.parseLong(fromUntil[1].trim()));
                offsetRange.calculateDelta();
                break;
                default:
                    break;
        }
    }


    @Test
    public void testParseOffsetRangeFromFile() throws IOException {
        String filePath = "testDir/offsetRangeMetadata.txt";
        Map<String, List<KafkaPartitionOffsetUtils.PartitionOffsetRange>> stringListMap = KafkaPartitionOffsetUtils.parseOffsetRangeFromFilePath(filePath);
        KafkaPartitionOffsetUtils.printOffsetRange(stringListMap);

    }

}
