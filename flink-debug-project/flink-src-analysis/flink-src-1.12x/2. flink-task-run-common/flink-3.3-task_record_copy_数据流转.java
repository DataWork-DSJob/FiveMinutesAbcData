

copy:107, MapSerializer (org.apache.flink.api.common.typeutils.base)
copy:43, MapSerializer (org.apache.flink.api.common.typeutils.base)
copyPositionBased:160, RowSerializer (org.apache.flink.api.java.typeutils.runtime)
copy:139, RowSerializer (org.apache.flink.api.java.typeutils.runtime)
copy:69, RowSerializer (org.apache.flink.api.java.typeutils.runtime)
pushToOperator:74, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)


CopyingChainingOutput.pushToOperator(){
	numRecordsOut.inc();
	numRecordsIn.inc();
	StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue())); {
		// Row , DataStream, Table 等的序列化
		RowSerializer.copy(from) {// Row copy(Row from) 
			Set<String> fieldNames = from.getFieldNames(false);
			if (fieldNames == null) {
				return copyPositionBased(from);{
					for (int i = 0; i < length; i++) {
						Object fromField = from.getField(i);
						Object copy = fieldSerializers[i].copy(fromField); {
							// Map类型,用 MapSerializer
							// 其他类型各自用其他Serializer
							
						}
						fieldByPosition[i] = copy;
					}
				}
			} else {
				return copyNameBased(from, fieldNames);
			}
		}
	}
	recordProcessor.accept(copy);
}