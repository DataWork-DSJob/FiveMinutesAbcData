

SELECT group_key, event_time, DATE_FORMAT(event_time, 'HH:mm:ss') as tim_millis, count(*) AS cnt, PROCTIME() AS query_time 
FROM ods_trade_csv 
GROUP BY group_key, event_time



dateFormat:398, SqlDateTimeUtils (org.apache.flink.table.runtime.functions)
processElement:-1, StreamExecCalc$40
pushToOperator:71, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:46, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:26, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:50, CountingOutput (org.apache.flink.streaming.api.operators)
collect:28, CountingOutput (org.apache.flink.streaming.api.operators)
collect:50, TimestampedCollector (org.apache.flink.streaming.api.operators)
processElement:194, GroupAggFunction (org.apache.flink.table.runtime.operators.aggregate)
processElement:43, GroupAggFunction (org.apache.flink.table.runtime.operators.aggregate)
processElement:83, KeyedProcessOperator (org.apache.flink.streaming.api.operators)
emitRecord:191, OneInputStreamTask$StreamTaskNetworkOutput (org.apache.flink.streaming.runtime.tasks)
processElement:204, StreamTaskNetworkInput (org.apache.flink.streaming.runtime.io)
emitNext:174, StreamTaskNetworkInput (org.apache.flink.streaming.runtime.io)


StreamTaskNetworkInput.processElement(StreamElement recordOrMark, DataOutput<T> output) {
	if (recordOrMark.isRecord()) {
		// 处理数据, 这里 
		output.emitRecord(recordOrMark.asRecord());{// OneInputStreamTask$StreamTaskNetworkOutput
			operator.processElement(record);{// KeyedProcessOperator.processElement()
				collector.setTimestamp(element);
				
				userFunction.processElement(element.getValue(), context, collector);{// GroupAggFunction.processElement()
					
				}
				
			}
		}
	} else if (recordOrMark.isWatermark()) {
		statusWatermarkValve.inputWatermark(
				recordOrMark.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
	} else if (recordOrMark.isLatencyMarker()) {
		output.emitLatencyMarker(recordOrMark.asLatencyMarker());
	} else if (recordOrMark.isStreamStatus()) {
		statusWatermarkValve.inputStreamStatus(
				recordOrMark.asStreamStatus(),
				flattenedChannelIndices.get(lastChannel),
				output);
	} else {
		throw new UnsupportedOperationException("Unknown type of StreamElement");
	}
}

//  table-sql api, flink-table-runtime 包 中的 GroupBy Agg 实现类:
org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction.processElement(){
	
	// 从状态中获取累加器; 
	RowData accumulators = accState.value();
	if (null == accumulators) {
		accumulators = function.createAccumulators();
	}
	
	// set accumulators to handler first
	function.setAccumulators(accumulators);
	// get previous aggregate result
	RowData prevAggValue = function.getValue();
	
	// get current aggregate result
    RowData newAggValue = function.getValue();
	
	// get accumulator
    accumulators = function.getAccumulators();
	
	boolean countIsZero = recordCounter.recordCountIsZero(accumulators);{
		
	}
	if (!countIsZero) {
		accState.update(accumulators);
		out.collect(resultRow);
	} else {
		accState.clear();
		function.cleanup();
	}
	
}





