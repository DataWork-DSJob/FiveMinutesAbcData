package flink.debug.paimon;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.debug.FlinkSqlTableCommon;
import flink.debug.pressure.SimpleSource;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class DysTradeCostByPaimon extends FlinkSqlTableCommon {

    @Override
    protected String getBootstrapServers() {
        return "192.168.51.112:9092";
    }

    protected String getPaimonPath() {
//        String path = "file:/tmp/paimon_f117";
        // hdfs://172.20.59.227:8888/user/myuser/output10
        String path = "hdfs://bdnode103:9000/tmp/paimon_idea";
        return path;
    }

    protected String getTemplateJson() {
        return "[\n" +
                "{\n" +
                "  \"msgId\":\"PktUp_2_短 282574499909166\",\n" +
                "  \"message\":\"2023-07-17 13:25:53.015798 msg : [msg_no:7390700: ver:2]<AgwMsgExtQueryAssetDebts:<AgwMsgExtQueryBase:<cust_id=\\\"03000051380130  \\\"|fund_account_id=\\\"03*****44\\\"|account_id=\\\"            \\\"|client_seq_id=16******3|client_feature_code=\\\"PC;IIP=113.204.232.50;IPORT=53607;LIP=172.21.1.148;MAC=8CEC4B7A836E;HD=WD-WCC6Y7UA6HRY;CPU=HZ__BFEBFBFF000906E9@XUNTOU-PB;V1.0.1.9524\\\"|user_info=\\\"                                                                \\\"|password=\\\"5e44e0b1080c5bfa07bcb720292167a808c9755a403f7fdc05322d16b7092baa\\\"|branch_id=\\\"3**2      \\\"|>currency=\\\"    \\\"|>PktUpIoReserve:<agw_seq_id=282574499909166|agw_user=\\\"user133\\\"|>\\n\",\n" +
                "  \"timestamp\":1689910795618,\n" +
                "  \"version\":\"1.0\",\n" +
                "  \"@timestamp\": \"2023-07-17T13:25:54.457798Z\",\n" +
                "  \"offset\": 109329323,\n" +
                "  \"source\": \"/abc/agw/wex/file/AGG_53_32_20230717_14523.txt\"\n" +
                "}\n" +
                ",\n" +
                "{\n" +
                "  \"msgId\":\"PktDown_2_超长_60compatible 282574499909166\",\n" +
                "  \"message\":\"2023-07-17 13:25:56.622133 msg : [msg_no:310003: ver:2]<AgwMsgExtQueryResultTradeOrder:<AgwMsgExtQueryResultBase:<cust_id=\\\"03000051380130  \\\"|fund_account_id=\\\"0332020010780937\\\"|client_seq_id=2981382966844998718|query_result_code=0|user_info=\\\"RZ                                                              \\\"|branch_id=\\\"3202      \\\"|key_error_msg=\\\"\\\"|key_error_code=\\\"\\\"|key_error_level=0|>last_index=2849|total_num=6266|<compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000022989751                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102343640|last_px=123900|last_qty=160000|total_value_traded=198240000|fee=237900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000022990159                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102343800|last_px=123900|last_qty=240000|total_value_traded=297360000|fee=356900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000022990174                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102343800|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44600|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000022992020                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102344300|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000022995114                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102345280|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023004697                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102348050|last_px=123900|last_qty=3160000|total_value_traded=3915240000|fee=4698200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023009556                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102349670|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023009559                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102349670|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44600|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023052814                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102401900|last_px=123900|last_qty=1340000|total_value_traded=1660260000|fee=1992400|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023271600                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102505490|last_px=123900|last_qty=160000|total_value_traded=198240000|fee=237800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023271885                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102505580|last_px=123900|last_qty=150000|total_value_traded=185850000|fee=223100|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023272052                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102505650|last_px=123900|last_qty=90000|total_value_traded=111510000|fee=133800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023275807                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102506760|last_px=123900|last_qty=260000|total_value_traded=322140000|fee=386500|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023276106                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102506880|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023297459                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102513010|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312395                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516570|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312533                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516600|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382185                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537280|last_px=123900|last_qty=290000|total_value_traded=359310000|fee=431100|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382565                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537370|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661264                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651230|last_px=123900|last_qty=3200000|total_value_traded=3964800000|fee=4757700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661298                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661301                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661304                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661307                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661310                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661313                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661316                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661319                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661322                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023276106                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102506880|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023297459                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102513010|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312395                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516570|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312533                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516600|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382185                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537280|last_px=123900|last_qty=290000|total_value_traded=359310000|fee=431100|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382565                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537370|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661264                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651230|last_px=123900|last_qty=3200000|total_value_traded=3964800000|fee=4757700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661298                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661301                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661304                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661307                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661310                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661313                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661316                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661319                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661322                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023276106                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102506880|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023297459                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102513010|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312395                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516570|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312533                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516600|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382185                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537280|last_px=123900|last_qty=290000|total_value_traded=359310000|fee=431100|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382565                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537370|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661264                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651230|last_px=123900|last_qty=3200000|total_value_traded=3964800000|fee=4757700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661298                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661301                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661304                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661307                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661310                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661313                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661316                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661319                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661322                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023276106                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102506880|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023297459                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102513010|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312395                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516570|last_px=123900|last_qty=140000|total_value_traded=173460000|fee=208200|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023312533                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102516600|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382185                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537280|last_px=123900|last_qty=290000|total_value_traded=359310000|fee=431100|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023382565                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102537370|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661264                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651230|last_px=123900|last_qty=3200000|total_value_traded=3964800000|fee=4757700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661298                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661301                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661304                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661307                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=30000|total_value_traded=37170000|fee=44700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661310                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661313                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661316                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661319                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661322                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14800|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661325                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661328                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=10000|total_value_traded=12390000|fee=14900|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661331                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=50000|total_value_traded=61950000|fee=74400|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661334                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=50000|total_value_traded=61950000|fee=74400|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|><compatible=1>AgwTradeOrderArray:<business_type=200|security_id=\\\"002527  \\\"|security_symbol=\\\"新时达                               \\\"|market_id=102|account_id=\\\"0605072485  \\\"|side=2|ord_type=2|exec_type=F|exec_id=\\\"0101000023661339                \\\"|cl_ord_no=610119709|order_id=\\\"0030RXWVAJ00RNN0\\\"|cl_ord_id=\\\"11000440IO\\\"|transact_time=20230717102651240|last_px=123900|last_qty=20000|total_value_traded=24780000|fee=29700|currency=\\\"CNY \\\"|order_price=123900|order_qty=30000000|bs_flag2=\\\"0S\\\"|user_info=\\\"RZ                                                              \\\"|credit_digest_id=u|match_type=0|orig_cl_ord_no=0|security_full_symbol=\\\"新时达                                                       \\\"|>>PktDownIoReserve:<agw_seq_id=282574499909166|>\",\n" +
                "  \"timestamp\":1689910795618,\n" +
                "  \"version\":\"1.0\",\n" +
                "  \"@timestamp\": \"2023-07-17T13:25:56.857798Z\",\n" +
                "  \"offset\": 109329323,\n" +
                "  \"source\": \"/abc/agw/wex/file/AGG_53_32_20230717_14523.txt\"\n" +
                "}\n" +
                "]\n";
    }

    private DataStreamSource<String> getSourceDStreamFromJsonCollect(StreamExecutionEnvironment env) {
        String templateJson = getTemplateJson().trim();
        DataStreamSource<String> source = env.addSource(new SimpleSource<>(-1, templateJson), TypeInformation.of(String.class));
        return source;
    }


    @Test
    public void testChangelogDataStreamSqlWindowAgg() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 5);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().addJobParameter("dys.collect.agg.config.json", "{}");
//        DataStreamSource<String> stringDataStream = getSourceDStreamFromKafka(env, "ods_trade_csv");
        DataStreamSource<String> stringDataStream = getSourceDStreamFromJsonCollect(env);

        DataStream<JSONObject> jsonDataStream = stringDataStream.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String line, Collector<JSONObject> collector) throws Exception {
                if (null != line && !line.isEmpty()) {
                    JSONArray jsonArray;
                    if (line.trim().startsWith("[")) {
                        try {
                            jsonArray = JSON.parseArray(line);
                        } catch (RuntimeException e) {
                            JSONObject jsonObject = JSON.parseObject(line);
                            jsonArray = new JSONArray();
                            jsonArray.add(jsonObject);
                        }
                    } else {
                        try {
                            JSONObject jsonObject = JSON.parseObject(line);
                            jsonArray = new JSONArray();
                            jsonArray.add(jsonObject);
                        } catch (RuntimeException e) {
                            jsonArray = JSON.parseArray(line);
                        }
                    }
                    if (!jsonArray.isEmpty()) {
                        for (Object ele: jsonArray) {
                            JSONObject json = (JSONObject) ele;
                            try {
                                collector.collect(json);
                            }catch (RuntimeException e) {
                                e.printStackTrace();
                                throw e;
                            }
                        }
                    }
                }
            }
        });

        jsonDataStream = jsonDataStream.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject json) throws Exception {
                Instant now = Instant.now();
                long curTimeSec = (TimeUnit.MILLISECONDS.toSeconds(now.toEpochMilli()) / 5) * 5;

                long curTimeMicro = TimeUnit.MILLISECONDS.toMicros(now.toEpochMilli()) + TimeUnit.NANOSECONDS.toMicros(now.getNano());

                String message = json.getString("message");
                int msgIndex = message.indexOf(" msg :");
                String tradeTimeStr = message.substring(0, msgIndex);
                Instant tradeInstant = ZonedDateTime.parse(tradeTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneId.systemDefault())).toInstant();
                long tradeTimeMicro = TimeUnit.MILLISECONDS.toMicros(tradeInstant.toEpochMilli()) + TimeUnit.NANOSECONDS.toMicros(tradeInstant.getNano());

                String collectTime = json.getString("@timestamp");
                Instant collectInstant = ZonedDateTime.parse(collectTime, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXXXX").withZone(ZoneId.systemDefault())).toInstant();
                long collectTimeMicro = TimeUnit.MILLISECONDS.toMicros(collectInstant.toEpochMilli()) + TimeUnit.NANOSECONDS.toMicros(collectInstant.getNano());

                // |>PktUpIoReserve:<agw_seq_id=282574499909166|agw_user=\"user133\"|>\n",
                int pktIndex = message.lastIndexOf(">Pkt");
                String pktMsg = message.substring(pktIndex + 1, message.lastIndexOf(">"));
                int pktSepIndex = pktMsg.indexOf(":<");
                String msgType = pktMsg.substring(0, pktSepIndex);
                String pktKvTuple = pktMsg.substring(pktSepIndex + 2);
                String[] split = pktKvTuple.split("\\|");
                for (String kv: split) {
                    String[] kvArr = kv.split("=");
                    if (kvArr.length >1) {
                        json.put(kvArr[0], kvArr[1]);
                    }
                }
                String agwSeqId = json.getString("agw_seq_id");

                json.put("msgType", msgType);
                json.put("pk", agwSeqId);

                json.put("pipeInTime", LocalDateTime.ofInstant(now, ZoneId.systemDefault()));
                json.put("pipeInTimeMicro", curTimeMicro);
                json.put("tradeTime", tradeTimeStr);
                json.put("tradeTimeMicro", tradeTimeMicro);
                json.put("collectTime", collectTime);
                json.put("collectTimeMicro", collectTimeMicro);
                json.put("pipeInLatency", tradeTimeMicro - collectTimeMicro);
                json.put("collectLatency", collectTimeMicro - tradeTimeMicro);

                json.put("curTimeSec", curTimeSec);
                return json;
            }
        });

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[] {Types.STRING, Types.LONG, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG,
                        Types.INT, Types.STRING, Types.MAP(Types.STRING,  Types.STRING)},
                new String[] {"agw_seq_id", "curTimeSec", "tradeTime","message", "msgType", "tradeTimeMicro", "collectTimeMicro", "pipeInTimeMicro", "pipeInLatency", "collectLatency",
                        "offset", "source", "others"}
        );

        SingleOutputStreamOperator<Row> dataStream = jsonDataStream.map(new RichMapFunction<JSONObject, Row>() {
            private String[] fieldNames = rowTypeInfo.getFieldNames();
            private TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
            @Override
            public Row map(JSONObject json) throws Exception {
                Row row = new Row(RowKind.INSERT, fieldNames.length);
                for (int i = 0; i < fieldNames.length; i++) {
                    String fieldName = fieldNames[i];
                    Object fieldVal = json.remove(fieldName);
                    if (null != fieldVal) {
                        TypeInformation fieldType = fieldTypes[i];
                        if (Types.STRING.equals(fieldType)) {
                            fieldVal = fieldVal.toString();
                        } else if (Types.LONG.equals(fieldType)) {
                            if (!(fieldVal instanceof Long)) {
                                fieldVal = Long.parseLong(fieldVal.toString());
                            }
                        }
                        row.setField(i, fieldVal);
                    }
                }
                JSONObject other = new JSONObject();
                json.forEach((k,v)-> {
                    other.put(k, v.toString());
                });
                row.setField(fieldNames.length -1, other);
                return row;
            }
        }, rowTypeInfo);

        Schema schema = Schema.newBuilder()
                .column("agw_seq_id", DataTypes.STRING())
                .column("curTimeSec", DataTypes.BIGINT())

                .column("tradeTime", DataTypes.STRING())
                .column("message", DataTypes.STRING())
                .column("msgType", DataTypes.STRING())
                .column("tradeTimeMicro", DataTypes.BIGINT())
                .column("collectTimeMicro", DataTypes.BIGINT())
                .column("pipeInTimeMicro", DataTypes.BIGINT())
                .column("pipeInLatency", DataTypes.BIGINT())
                .column("collectLatency", DataTypes.BIGINT())

                .column("offset", DataTypes.INT())
                .column("source", DataTypes.STRING())
                .column("others", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                .columnByExpression("event_time", "TO_TIMESTAMP(`tradeTime`, 'yyyy-MM-dd HH:mm:ss.SSSSSS')")
                .watermark("event_time", "event_time - INTERVAL '10' SECOND ")
                .build();

        Table table = tableEnv.fromChangelogStream(dataStream, schema);
        tableEnv.createTemporaryView("ods_dys_log", table);
        tableEnv.createTemporaryFunction("collect_field", CollectAggregateFunc.class);

        tableEnv.executeSql("CREATE VIEW dws_trade_summary_10s AS \n" +
                "SELECT\n" +
                "  window_start AS win_start, agw_seq_id, curTimeSec,\n" +
                "  count(*) as cnt,\n" +
                "  collect_field('message,msgType', message, msgType) AS collect_values \n" +
                "FROM TABLE(TUMBLE(TABLE ods_dys_log, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)) \n" +
                "GROUP BY window_start, agw_seq_id, curTimeSec ");

        Table newTable = tableEnv.sqlQuery("select * from dws_trade_summary_10s");

        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(newTable);
        DataStream<Row> rowDataStreamV2 = rowDataStream.process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
                Long timestamp = ctx.timestamp();
                long curProcTime = ctx.timerService().currentProcessingTime();
                long curWm = ctx.timerService().currentWatermark();

                Row of = Row.of(curProcTime, curWm);
                Row newRow = Row.join(value, of);
                try {
                    out.collect(newRow);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }, new RowTypeInfo(
                new TypeInformation[] {Types.LOCAL_DATE_TIME, Types.STRING, Types.LONG, Types.LONG, Types.ROW(Types.STRING), Types.LONG, Types.LONG},
                new String[] {"win_start", "agw_seq_id", "curTimeSec", "cnt", "collect_values", "curProcTime", "curWatermark"}
        ));

        String paimonPath = getPaimonPath();
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='" + paimonPath + "')");
        tableEnv.executeSql("USE CATALOG paimon");

        Table winAggTable = tableEnv.fromChangelogStream(rowDataStreamV2, Schema.newBuilder()
                .column("win_start", DataTypes.TIMESTAMP(3))
                .column("agw_seq_id", DataTypes.STRING())
                .column("curTimeSec", DataTypes.BIGINT())
                .column("cnt", DataTypes.BIGINT())
                .column("collect_values", DataTypes.ROW(DataTypes.STRING()))

                .column("curProcTime", DataTypes.BIGINT())
                .column("curWatermark", DataTypes.BIGINT())
                .watermark("win_start", "win_start - INTERVAL '10' SECOND ")
                .build()
        );
        tableEnv.createTemporaryView("dws_trade_summary_10s_fromDs", winAggTable);

        tableEnv.executeSql("select * from dws_trade_summary_10s_fromDs").print();

        tableEnv.executeSql("DROP TABLE IF EXISTS dws_trade_summary_10s_paimon");

        tableEnv.executeSql("CREATE TABLE dws_trade_summary_10s_paimon (\n" +
                "    win_time TIMESTAMP(3),\n" +
                "    agw_seq_id STRING,\n" +
                "    curTimeSec BIGINT,\n" +
                "    cnt BIGINT,\n" +
                "    collect_values ROW<STRING>,\n" +
                "    pk STRING,\n" +
                "    PRIMARY KEY (`pk`) NOT ENFORCED,\n" +
                "    WATERMARK FOR win_time AS win_time\n" +
                ")");

        tableEnv.executeSql("INSERT INTO dws_trade_summary_10s_paimon\n" +
                "SELECT win_time, agw_seq_id, curTimeSec, cnt, collect_values, \n" +
                "       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss'), agw_seq_id, CAST(curTimeSec AS STRING)) pk\n" +
                "FROM dws_trade_summary_10s_fromDs ");

        Table sinkTable = tableEnv.sqlQuery("SELECT * FROM dws_trade_summary_10s_paimon");
        DataStream<Row> sinkDataStream = tableEnv.toChangelogStream(sinkTable);

        DataStream<Row> sinkDataStreamProcess = sinkDataStream.process(new ProcessFunction<Row, Row>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
                long mr = ctx.timerService().currentWatermark();

                ctx.timerService().registerEventTimeTimer(1L);
                try {
                    out.collect(value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        sinkDataStreamProcess.executeAndCollect().forEachRemaining(row -> {
            RowKind kind = row.getKind();
            if (kind.equals(RowKind.INSERT) || RowKind.UPDATE_AFTER.equals(kind)) {
                System.out.println(row.getKind() + " => " + row);
            } else {

            }
        });



    }

    private DataStreamSource<String> getSourceDStreamFromKafka(StreamExecutionEnvironment env, String topic) {
        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getSimpleName());
        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), kafkaProps);
        DataStreamSource<String> stringDataStream = env.addSource(kafka);
        return stringDataStream;
    }

    @Test
    public void testWatermark() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableChangelogStateBackend(true);
        Path dirPath = getOrCreateDirFromUserDir("checkpoint-rocksdb");
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(dirPath.toUri().toString(), true);
//        rocksDBStateBackend.setDbStoragePath("hdfs://bdnode124:9009/user/root/flink/rocksdb");// has a non-local scheme
        rocksDBStateBackend.setDbStoragePath("D:\\tmp\\flink_rocksdb\\db1");
        env.setStateBackend(rocksDBStateBackend);
//        env.setStateBackend(new FsStateBackend(dirPath.toUri().toString()));
        env.enableCheckpointing(1000*3, CheckpointingMode.EXACTLY_ONCE);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // A,2023-10-04T23:07:15,1
        DataStreamSource<String> lines = env.socketTextStream("bdnode112", 9909);

        lines
                .flatMap(new FlatMapFunction<String, Row>() {
                    @Override
                    public void flatMap(String line, Collector<Row> out) throws Exception {
                        if (null != line && !line.isEmpty()) {
                            String[] split = line.split(",");
                            if (split.length > 0) {
                                Row row;
                                if (split.length == 1) {
                                    String numStr = split[0].trim();
                                    Object num;
                                    try {
                                        num = Integer.parseInt(numStr);
                                    } catch (RuntimeException e) {
                                        try {
                                            num = new BigDecimal(numStr).doubleValue();
                                        } catch (RuntimeException e2) {
                                            num = numStr;
                                        }
                                    }
                                    row = Row.of(num);
                                } else {
                                    // 有key,
                                    row = new Row(split.length);
                                    for (int i = 0; i < split.length; i++) {
                                        String ele = split[i];
                                        row.setField(i, ele);
                                    }
                                }
                                // 输出
                                try {
                                    out.collect(row);
                                } catch (RuntimeException e) {
                                    e.printStackTrace();
                                }

                            }
                        }
                    }
                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Row>forMonotonousTimestamps()
//                        .withTimestampAssigner((e,t) -> System.currentTimeMillis()))
//                .assignTimestampsAndWatermarks(new MyPunctuatedWatermarksAssignerAscendingTimestamp())
//                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Row>() {
//                    @Override
//                    public long extractTimestamp(Row element, long recordTimestamp) {
//                        return 0;
//                    }
//
//                    @Nullable
//                    @Override
//                    public Watermark checkAndGetNextWatermark(Row lastElement, long extractedTimestamp) {
//                        return null;
//                    }
//                })
//              //  容忍乱序数据, 提取 timestamp
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Row>forBoundedOutOfOrderness(Duration.ofMillis(1000*5))
//                        .withTimestampAssigner((event, time) -> {
//                            long ts = System.currentTimeMillis();
//                            if (event.getArity() > 1) {
//                                Object secondAsTs = event.getField(1);
//                                if (null != secondAsTs) {
//                                    if (secondAsTs instanceof Number) {
//                                        ts = (long) ((Number) secondAsTs).doubleValue();
//                                    } else if (secondAsTs instanceof String) {
//                                        try {
//                                            LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
//                                            ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
//                                        } catch (RuntimeException e) {
//                                            e.printStackTrace();
//                                        }
//                                    }
//                                }
//                            }
//                            return ts;
//                        })
//                )
                // 完全自定义 WatermarkGenerator  & TimestampAssigner
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Row>() {
                    @Override
                    public WatermarkGenerator<Row> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Row>() {
                            private final long outOfOrdernessMillis = 1000 * 0;
                            private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
                            @Override
                            public void onEvent(Row event, long eventTimestamp, WatermarkOutput output) {
                                if (eventTimestamp > maxTimestamp) {
                                    System.out.println(String.format("tid=%d 最大时间戳将更新(from %s to %s) \t event=%s",
                                            Thread.currentThread().getId(),
                                            new Date(maxTimestamp).toLocaleString(),
                                            new Date(eventTimestamp).toLocaleString(),
                                            event
                                    ));
                                }
                                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                            }
                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                long curWatermark = maxTimestamp - outOfOrdernessMillis - 1;
                                output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(curWatermark));
                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<Row> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new TimestampAssigner<Row>() {
                            @Override
                            public long extractTimestamp(Row event, long recordTimestamp) {
                                long ts = System.currentTimeMillis();
                                if (event.getArity() > 1) {
                                    Object secondAsTs = event.getField(1);
                                    if (null != secondAsTs) {
                                        if (secondAsTs instanceof Number) {
                                            ts = (long) ((Number) secondAsTs).doubleValue();
                                        } else if (secondAsTs instanceof String) {
                                            try {
                                                LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
                                                ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
                                            } catch (RuntimeException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    }
                                }
                                return ts;
                            }
                        };
                    }
                })
                // // 无水位, 不过期
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>noWatermarks()
//                        .withTimestampAssigner((event, time) -> {
//                            long ts = System.currentTimeMillis();
//                            if (event.getArity() > 1) {
//                                Object secondAsTs = event.getField(1);
//                                if (null != secondAsTs) {
//                                    if (secondAsTs instanceof Number) {
//                                        ts = (long) ((Number) secondAsTs).doubleValue();
//                                    } else if (secondAsTs instanceof String) {
//                                        try {
//                                            LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
//                                            ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
//                                        } catch (RuntimeException e) {
//                                            e.printStackTrace();
//                                        }
//                                    }
//                                }
//                            }
//                            return ts;
//                        })
//                )

                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        String pk;
                        if (row.getArity() <= 1) {
                            pk = "";
                        } else {
                            Object firstValue = row.getField(0);
                            if (null != firstValue && firstValue instanceof String) {
                                pk = (String) firstValue;
                            } else {
                                // 若第一个元素 字符串, 则未设置分组, 使用 统一默认 分组;
                                pk = "";
                            }
                        }
                        return pk;
                    }
                })
//                .window(TumblingProcessingTimeWindows.of(
//                        Time.seconds(10),
//                        Time.seconds(0),// 窗口从偏移多长开始; 如 (1h,15m)则 start at 0:15:00,1:15:00,2:15:00,etc
//                        //ALIGNED 对其:  all panes fire at the same time across all partitions.
//                        // RANDOM :
//                        WindowStagger.ALIGNED) //  The utility that produces staggering offset  交错 staggers offset for each window assignment
//                )

                .window(TumblingEventTimeWindows.of(
                        Time.seconds(10), Time.seconds(0),
                        WindowStagger.ALIGNED
                ))
//                .trigger(CountTrigger.of(3))
                .trigger(new Trigger<Row, TimeWindow>() {
                    private static final long serialVersionUID = 1L;
                    private int count = 0;
                    private static final int MAX_COUNT = 3;
                    //每个元素被添加到窗口时都会调用该方法
                    @Override
                    public TriggerResult onElement(Row element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 这个是干嘛?
                        ctx.registerEventTimeTimer(window.maxTimestamp());
                        // CONTINUE是代表不做输出，也即是，此时我们想要实现比如100条输出一次，
                        // 而不是窗口结束再输出就可以在这里实现。
                        TriggerResult triggerResult;
                        count++;
                        if(count > MAX_COUNT){
                            count = 0;
                            triggerResult = TriggerResult.FIRE;
                        } else {
                            triggerResult = TriggerResult.CONTINUE;
                        }

                        System.out.println(String.format("onElement, tid=%d, curWm=%s, winStart=%s, count=%d, trigger=%s; \t Record=%s",
                                Thread.currentThread().getId(),
                                new Date(ctx.getCurrentWatermark()).toLocaleString(),
                                new Date(window.getStart()).toLocaleString(),
                                count,
                                this,
                                element
                        ));
                        return triggerResult;
                    }

                    // registerProcessingTimeTimer() ? 注册的系统时间计时器触发时，将调用onEventTime（）方法;  Called when a processing-time timer that was set using the trigger context fires
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        String curWm = new Date(ctx.getCurrentWatermark()).toLocaleString();
                        String curTime = new Date().toLocaleString();

                        System.out.println(String.format("Window.onEventTime(水位更新,或者注册实际到), tid=%d, curWm=%s, curTime=%s",
                                Thread.currentThread().getId(),
                                curWm, curTime
                        ));
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
                    }

                    @Override
                    public void onMerge(TimeWindow window, OnMergeContext ctx) {
                        // only register a timer if the time is not yet past the end of the merged window
                        // this is in line with the logic in onElement(). If the time is past the end of
                        // the window onElement() will fire and setting a timer here would fire the window twice.
                        long windowMaxTimestamp = window.maxTimestamp();
                        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
                        }
                    }

                })
                .aggregate(new AggregateFunction<Row, JSONObject, Row>() {
                    @Override
                    public JSONObject createAccumulator() {
                        JSONObject acc = new JSONObject();
                        acc.put("count", 0L);
                        acc.put("elementList", new JSONArray());
                        return acc;
                    }

                    @Override
                    public JSONObject add(Row value, JSONObject accumulator) {
                        if (null != value) {
                            long count = accumulator.getLongValue("count");
                            count++;
                            accumulator.put("count",count);
                            if (value.getArity() > 2) {
                                JSONArray elementList = accumulator.getJSONArray("elementList");
                                if (null == elementList) {
                                    elementList = new JSONArray();
                                    accumulator.put("elementList", elementList);
                                }
                                elementList.add(value.getField(2));
                            }
                        }
                        return accumulator;
                    }

                    @Override
                    public Row getResult(JSONObject accumulator) {
                        long count = accumulator.getLongValue("count");
                        JSONArray elementList = accumulator.getJSONArray("elementList");
                        return Row.of(count, elementList);
                    }

                    @Override
                    public JSONObject merge(JSONObject a, JSONObject b) {
                        a.put("count", a.getLongValue("count") + b.getLongValue("count"));
                        return a;
                    }
                }, new ProcessWindowFunction<Row, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Row, JSONObject, String, TimeWindow>.Context context, Iterable<Row> elements, Collector<JSONObject> out) throws Exception {
                        Iterator<Row> it = elements.iterator();
                        while (it.hasNext()) {
                            Row row = it.next();
                            JSONObject outRecord = new JSONObject();
                            outRecord.put("groupKey", key);
                            outRecord.put("curWatermark", context.currentWatermark() +" : " + new Date(context.currentWatermark()).toLocaleString());
                            outRecord.put("curProcTime", context.currentProcessingTime() +" : " + new Date(context.currentProcessingTime()).toLocaleString());
                            outRecord.put("win_start", new Date(context.window().getStart()).toLocaleString());

                            for (int i = 0; i < row.getArity(); i++) {
                                Object fValue = row.getField(i);
                                outRecord.put("rowIdx_" + i, fValue);
                            }
                            out.collect(outRecord);
                        }
                    }
                })
                .print()
        ;

        env.execute("Flink Streaming Java API Skeleton");
    }


}
