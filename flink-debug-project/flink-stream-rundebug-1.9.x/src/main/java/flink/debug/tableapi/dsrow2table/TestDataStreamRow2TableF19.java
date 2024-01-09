package flink.debug.tableapi.dsrow2table;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestDatastreamRow2Table
 * @description: flink.debug.tableapi.dsrow2table.TestDatastreamRow2Table
 * @author: jiaqing.he
 * @date: 2023/11/26 9:24
 * @version: 1.0
 */
public class TestDataStreamRow2TableF19 {

    class MyTestUpsertSink implements UpsertStreamTableSink<Row> {

        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        public MyTestUpsertSink(String[] fieldNames, TypeInformation[] typeInformations) {
            this.fieldNames = fieldNames;
            this.fieldTypes = typeInformations;
        }

        @Override
        public void setKeyFields(String[] keys) {

        }

        @Override
        public void setIsAppendOnly(Boolean isAppendOnly) {

        }

        @Override
        public TypeInformation<Row> getRecordType() {
            return new RowTypeInfo(fieldTypes, fieldNames);
        }

        @Override
        public DataType getConsumedDataType() {
            return UpsertStreamTableSink.super.getConsumedDataType();
        }

        @Override
        public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
            return UpsertStreamTableSink.super.getOutputType();
        }

        @Override
        public TableSchema getTableSchema() {
            return new TableSchema(fieldNames, fieldTypes);
        }

        @Override
        public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
            consumeDataStream(dataStream);
        }

        @Override
        public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
//            DataStreamSink<Tuple2<Boolean, Row>> dsSink = dataStream.addSink(new MyTestSinkFunc());
            DataStreamSink<Tuple2<Boolean, Row>> dsSink = dataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                @Override
                public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                    System.out.println(value.f0 + " : " + value.f1);
                }
            });
            return dsSink;
        }

        @Override
        public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            MyTestUpsertSink upsertSink = new MyTestUpsertSink(fieldNames, fieldTypes);
            return upsertSink;
        }

        @Override
        public String[] getFieldNames() {
            return fieldNames;
        }

        @Override
        public TypeInformation<?>[] getFieldTypes() {
            return fieldTypes;
        }

    }

    class MyStreamTableSource implements StreamTableSource<Row>
//            ,DefinedProctimeAttribute
            , DefinedFieldMapping
        , DefinedRowtimeAttributes
    {
        DataStream<Row> dataStream;
        private TableSchema tableSchema;
        String[] fieldNames = new String[]{"name", "rowtime", "score"};
        TypeInformation[] fieldTypes;

        public MyStreamTableSource(DataStream<Row> dataStream) {
            this.dataStream = dataStream;

        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
            return dataStream;
        }
        @Override
        public TableSchema getTableSchema() {
            TableSchema tableSchema = new TableSchema(
                    fieldNames,
                    new TypeInformation[]{Types.STRING, Types.SQL_TIMESTAMP, Types.INT});
            return tableSchema;
        }


        @Override
        public TypeInformation<Row> getReturnType() {
//            TypeInformation<Row> rowType = tableSchema.toRowType();
            TypeInformation<Row> row = Types.ROW(Types.STRING, Types.LONG, Types.INT);
            return row;
        }
        @Override
        public String explainSource() {
            return "TestDataStream2TableSource";
        }


        @Override
        public Map<String, String> getFieldMapping() {
            Map<String, String> mapping = new HashMap<>();
            for (int i = 0; i < fieldNames.length; i++) {
                String fieldName = fieldNames[i];
                mapping.put(fieldName, "f" + i);
            }
            return mapping;
        }

//        @Nullable
//        @Override
//        public String getProctimeAttribute() {
//            return "proc_time";
//        }

        @Override
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
            return Collections.singletonList(
                    new RowtimeAttributeDescriptor(
                            "rowtime",
                            new ExistingField("ts"),
                            new BoundedOutOfOrderTimestamps(100)));
        }

    }

    @Test
    public void fromDataStream2TableByTableEnv() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Alice", 100));

        String[] fieldNames = {"name", "score"};

        TypeInformation[] fieldTypes = {BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};
        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Table inputTable = streamTableEnv.fromDataStream(dataStream, String.join(",", fieldNames));
        tbEnv.registerTable("input", inputTable);
        tbEnv.registerTableSink("output", new MyTestUpsertSink(
                new String[]{"name", "score"},
                fieldTypes
        ));
        tbEnv.insertInto(inputTable, "output");

        // 2. by TableEnvironment. DataStream To Sink
        DataStream<Row> dataStream2 = env.fromElements(
                Row.of("Lily", 101000L, 32),
                Row.of("Lily2", 202000L, 322),
                Row.of("Alice", 303000L, 100));
        Table table2FromTbSrc = tbEnv.fromTableSource(new MyStreamTableSource(dataStream2));
        tbEnv.registerTable("intputTable2", table2FromTbSrc);

        tbEnv.registerTableSink("outputTable2", new MyTestUpsertSink(
                new String[]{"name", "rowtime", "score"},
                fieldTypes));
        tbEnv.insertInto(table2FromTbSrc, "outputTable2");

        tbEnv.execute("test");

    }


    @Test
    public void fromTable2SinkByTableEnv() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));

        String[] fieldNames = {"name", "score"};
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Table inputTable = streamTableEnv.fromDataStream(dataStream, String.join(",", fieldNames));
        tbEnv.registerTable("input", inputTable);

        tbEnv.registerTableSink("output", new MyTestUpsertSink(
                new String[]{"name", "score"},
                new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO}
        ));
        tbEnv.insertInto(inputTable, "output");

//        streamTableEnv.insertInto();

        tbEnv.execute("test");

    }


    @Test
    public void testTableSink2_TableToPrintByTbEnvRegisterTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));

        String[] fieldNames = {"name", "score"};
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Table inputTable = streamTableEnv.fromDataStream(dataStream, String.join(",", fieldNames));
        tbEnv.registerTable("input", inputTable);

        tbEnv.registerTableSink("output", new MyTestUpsertSink(
                new String[]{"name", "score"},
                new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO}
        ));
        tbEnv.insertInto(inputTable, "output");

//        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = streamTableEnv.toRetractStream(inputTable, Row.class);


        tbEnv.execute("test");

    }

    @Test
    public void testTableSink3_StreamTbEnv_toRetractStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};
        String[] fieldNames = {"name", "score"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        DataStream<Row> dataStream = env.fromCollection(Arrays.asList(
                        Row.of("Alice", 200),
                        Row.of("Bob", 500),
                        Row.of("Alice", 100)),
                rowTypeInfo);

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
//        Table inputTable = streamTableEnv.fromDataStream(dataStream, String.join(",", fieldNames)).as("name,score");
        Table inputTable = streamTableEnv.fromDataStream(dataStream);
        tbEnv.registerTable("input", inputTable);


        // 2. 基于 StreamTableEnvironment.toRetractStream()
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = streamTableEnv.toRetractStream(inputTable, Row.class);

        tuple2DataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                System.out.println(value.f0 + " -> " + value.f1);
            }
        });

        Table queryTable = tbEnv.sqlQuery("select name, count(*) cnt, sum(score) sc_sum from input group by name");
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });


        tbEnv.execute("test");

    }


    @Test
    public void testDsSourceToTable1_StreamTbEnv_fromDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 200),
                Row.of("Bob", 500),
                Row.of("Alice", 100));

        String[] fieldNames = {"name", "score"};
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
//        Table inputTable = streamTableEnv.fromDataStream(dataStream, String.join(",", fieldNames));
        Table inputTable = streamTableEnv.fromDataStream(dataStream);
        tbEnv.registerTable("input", inputTable);

        Table queryTable = tbEnv.sqlQuery("select name, count(*) cnt, sum(score) sc_sum from input group by name");
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        tbEnv.execute("test");

    }


    @Test
    public void testDsSourceToTable2_TbEnv_fromTableSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 200),
                Row.of("Bob", 500),
                Row.of("Alice", 100));

        String[] fieldNames = {"name", "score"};
        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.INT};
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        Table table2FromTbSrc = tbEnv.fromTableSource(new StreamTableSource<Row>() {
            @Override
            public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
                return dataStream;
            }
            @Override
            public TableSchema getTableSchema() {
                TableSchema tableSchema = new TableSchema(fieldNames, types);
                return tableSchema;
            }

            @Override
            public DataType getProducedDataType() {
                DataType producedDataType = StreamTableSource.super.getProducedDataType();
                DataType rowType = DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT())
                );
                return producedDataType;
            }

            @Override
            public TypeInformation<Row> getReturnType() {
                TypeInformation<Row> type = dataStream.getType();
                TypeInformation<Row> row = Types.ROW(Types.STRING, Types.LONG, Types.INT);
                DataType rowType = DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT())
                );
                return type;
            }
        });
        tbEnv.registerTable("input", table2FromTbSrc);

        Table queryTable = tbEnv.sqlQuery("select f0, count(*) cnt, sum(f1) sc_sum from input group by f0");
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        tbEnv.execute("test");

    }


    @Test
    public void testDsSourceToTable3_TbEnv_registerDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};
        String[] fieldNames = {"name", "score"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        DataStream<Row> dataStream = env.fromCollection(Arrays.asList(
                Row.of("Alice", 200),
                Row.of("Bob", 500),
                Row.of("Alice", 100)),
                rowTypeInfo);

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

//        streamTableEnv.registerDataStream("input", dataStream, "name, score");
        streamTableEnv.registerDataStream("input", dataStream);

        Table queryTable = tbEnv.sqlQuery("select name, count(*) cnt, sum(score) sc_sum from input group by name");
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        env.execute("test");

    }


}
