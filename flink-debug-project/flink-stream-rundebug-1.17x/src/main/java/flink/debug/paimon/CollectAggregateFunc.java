package flink.debug.paimon;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * @projectName: FiveMinutesAbcData
 * @className: WeightedAvg
 * @description: flink.apistudy.table.udf.WeightedAvg
 * @author: jiaqing.he
 * @date: 2023/3/25 15:59
 * @version: 1.0
 */
@FunctionHint(
        output = @DataTypeHint("ROW<name STRING>")
)
public class CollectAggregateFunc extends AggregateFunction<Row, List<String>> {

    static class AccItem implements Serializable {
        String str;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        String json = context.getJobParameter("dys.collect.agg.config.json", "");
        if (null != json) {
            JSONObject jsonObject = JSON.parseObject(json);

        }
    }

    @Override
    public List<String> createAccumulator() {
        return new LinkedList<>();
    }

    @Override
    public Row getValue(List<String> accumulator) {
        return Row.of("myName");
    }

    public void accumulate(List<String> acc, @DataTypeHint("STRING") String fields, @DataTypeHint(inputGroup = InputGroup.ANY) Object... inputs) {
        if (null != inputs) {
            HashMap<Object, Object> map = new HashMap<>(inputs.length);

        }
//        if (null != value) {
//            acc.add(value.toString());
//        }

    }

//    @Override
//    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
//        TypeInference typeInference = TypeInference.newBuilder()
//                .inputTypeStrategy(
//                        new InputTypeStrategy() {
//                            @Override
//                            public ArgumentCount getArgumentCount() {
//                                return new ArgumentCount() {
//                                    @Override
//                                    public boolean isValidCount(int count) {
//                                        return false;
//                                    }
//
//                                    @Override
//                                    public Optional<Integer> getMinCount() {
//                                        return Optional.of(2);
//                                    }
//
//                                    @Override
//                                    public Optional<Integer> getMaxCount() {
//                                        return Optional.of(10);
//                                    }
//                                };
//                            }
//
//                            @Override
//                            public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
//                                return Optional.empty();
//                            }
//
//                            @Override
//                            public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
//                                return null;
//                            }
//                        }
//                )
//                .accumulatorTypeStrategy(
//                        callContext -> {
//                            DataType dataType = DataTypes.STRUCTURED(
//                                    LastDatedValueFunction.Accumulator.class,
//                                    DataTypes.FIELD("str", DataTypes.STRING()));
//                            return Optional.of(dataType);
//                        }
//                )
//                .outputTypeStrategy(
//                        callContext -> {
//                            DataType dataType = DataTypes.ROW(
//                                    DataTypes.FIELD("name", DataTypes.STRING()),
//                                    DataTypes.FIELD("cnt", DataTypes.BIGINT())
//                            );
//                            return Optional.of(dataType);
//                        }
//                )
//                .build();
//        return typeInference;
//    }


    public void retract(List<String> acc, @DataTypeHint("STRING") String fields, @DataTypeHint(inputGroup = InputGroup.ANY) Object... inputs) {

    }

//    public void merge(List<String> acc, Iterable<List<String>> it) {
//
//    }
//
//    public void resetAccumulator(List<String> acc) {
//
//    }


    @Override
    public TypeInformation<Row> getResultType() {
        return TypeInformation.of(new TypeHint<Row>(){});
    }

    @Override
    public TypeInformation<List<String>> getAccumulatorType() {
        return TypeInformation.of(new TypeHint<List<String>>(){});
    }

}
