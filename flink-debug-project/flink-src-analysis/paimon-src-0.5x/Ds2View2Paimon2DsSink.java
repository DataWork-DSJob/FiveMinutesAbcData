
	对于下面的一个作业; DataStream 2 TableView, to groupby SQL, to Paimon, to Select * from tb_paimon, to PrintSink; 
	// 总共3个作业; 
		第一个Task: 
			
			MyDataSource 
			
			TableScala
			StreamExecCalc
			
		
		第二个Task:
			GroupAggFunction
			StreamExecCalc.$ SQL代码生产类; 
			ConstraintEnforcer
		
		第三个Task : 
			
			RowDataStoreWriteOperator
			
			PaimonSource 
			KeyValueDataFileRecordReader
			
			FlinkRecordsWithSplitIds
			
			
			
		
		DataStream<Row> dataStream =env.addSource(new MySourceFunction<Row>() , rowTypeInfo).name("myTradeDetailStreamSrc");
        Table table = tableEnv.fromChangelogStream(dataStream);
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+ getPaimonPath() +"')");
        tableEnv.executeSql("USE CATALOG paimon");
        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.executeSql("drop table if exists dws_trade_paimon");
        tableEnv.executeSql("create table dws_trade_paimon (\n" +
                "  trade_id STRING,\n" +
                "  client_id STRING,\n" +
                "  cnt BIGINT,\n" +
                "  ta_sum DOUBLE,\n" +
                "  PRIMARY KEY (`trade_id`, `client_id`) NOT ENFORCED\n" +
                ")");
        tableEnv.executeSql("insert into dws_trade_paimon SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM InputTable GROUP BY trade_id, client_id");

		Table sqlQueryTable = tableEnv.sqlQuery("SELECT * FROM dws_trade_paimon");
        tableEnv.toRetractStream(sqlQueryTable, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, SinkFunction.Context context) throws Exception {
                System.out.println(value.f0 + " -> " + value.f1);
            }
        });


1. 对于 Ds2TempView2GroupBy2Paimon 的3个Task作业; 第二个Task GroupAggFunction 结果shuffle by bucket到 Write 
// 第一个Task DataStream to Table  
MySource.collect(){
	recordProcessor.accept(copy); 
		InputConversionOperator.processElement(); {
			// 1. 先吧 Row镀锡 
			Object internalRecord = converter.toInternal(externalRecord);  {// DataStructureConverterWrapper.toInternal();
				RowRowConverter.toInternal(Row external){
					GenericRowData genericRow = new GenericRowData(external.getKind(), length);
					Set<String> fieldNames = external.getFieldNames(false);
					// position-based field access
					if (fieldNames == null) {// 似乎默认 fieldNames ==null;
						for (int pos = 0; pos < length; pos++) {
							final Object value = external.getField(pos);
							genericRow.setField(pos, fieldConverters[pos].toInternalOrNull(value));{
								// 字符串的话,就是 StringStringConverter
								StringStringConverter.toInternal(){
									StringData.fromString(external);{
										return BinaryStringData.fromString(str);
									}
									// Integer, Long, Double 等是 IdentityConverter 实现返回; 
									IdentityConverter.toInternal(I external) {
										return external;
									}
									
									if (!produceRowtimeMetadata) {
										output.collect(outRecord.replace(payloadRowData));
										return;
									}
									
									final JoinedRowData joinedRowData = new JoinedRowData(kind, payloadRowData, rowtimeRowData);
									output.collect(outRecord.replace(joinedRowData)); {
										recordProcessor.accept(copy);
											// 这里进入SQL生产的函数; 
											StreamExecCalc.processElement();
									}
									
								}
							}
						}
					} else {
						// name-based field access
						for (String fieldName : fieldNames) {
							final Integer targetPos = positionByName.get(fieldName);
							if (targetPos == null) {
								throw new IllegalArgumentException(String.format("Unknown field name '%s' for mapping to a row position. Available names are: %s", fieldName, positionByName.keySet()));
							}
							final Object value = external.getField(fieldName);
							genericRow.setField(targetPos, fieldConverters[targetPos].toInternalOrNull(value));
						}
					}
					return genericRow;
				}
			
			}
			
		}
}

2. 对于 Ds2TempView2GroupBy2Paimon 的3个Task作业; 第二个Task GroupAggFunction 结果shuffle by bucket到 Write 

// shuffle by bucket 过程 
ConstraintEnforcer.processElement(){
	collect.collect()
		ChannelSelectorRecordWriter.emit()
			BufferWritingResultPartition.emitRecord()
}



3.1 Paimon 持久化, 写出逻辑; 

org.apache.paimon.flink.sink.RowDataStoreWriteOperator.processElement(){
	record = this.write.write(new FlinkRowWrapper((RowData)element.getValue()));
}

write:147, MergeTreeWriter (org.apache.paimon.mergetree)
write:53, MergeTreeWriter (org.apache.paimon.mergetree)
write:114, AbstractFileStoreWrite (org.apache.paimon.operation)
writeAndReturn:114, TableWriteImpl (org.apache.paimon.table.sink)
write:159, StoreSinkWriteImpl (org.apache.paimon.flink.sink)
processElement:123, RowDataStoreWriteOperator (org.apache.paimon.flink.sink)
accept:-1, 250688861 (org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$2600)


// 3.2 Checkpoint 完成后, 触发写出; 
StreamTask.performCheckpoint(){
	PrepareCommitOperator.prepareSnapshotPreBarrier() {
		for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators()) {
            if (!operatorWrapper.isClosed()) {
				// 这里就是具体算子的 准备快照了; Paimon的算子是 PrepareCommitOperator
                operatorWrapper.getStreamOperator().prepareSnapshotPreBarrier(checkpointId); {
					
					PrepareCommitOperator.prepareSnapshotPreBarrier()
						this.emitCommittables(false, checkpointId);{
							// 1. RowDataStoreWriteOperator.prepareCommit();
							List<Committable> commits = this.prepareCommit(waitCompaction, checkpointId); {
								List<Committable> committables = super.prepareCommit(waitCompaction, checkpointId);
								 this.logCallback.offsets().forEach((k, v) -> {
									committables.add(new Committable(checkpointId, Kind.LOG_OFFSET, new LogOffsetCommittable(k, v)));
								});
								return committables;
							}
							commits.forEach((committable) -> {
								// 这里应该是往下面输出; 
								this.output.collect(new StreamRecord(committable)); {
									// 会走到SinkFunction.invoke(value);
								}
								
							});
						}
					}
					
				}
            }
        }
	}
		
}


// SELECT * FROM dws_trade_paimon ; MySinkPrint 

4. 又从Paimon 中读取数据到DataStream 并处理 ; 线程名字叫: ChangeLogNormalize -> SinkConversion ; 
- 这个是怎么触发的呢? 难道是哪个SQL ? 


invoke:446, FlinkSQLPaimonExampleF117$4 (flink.debug.paimon)
invoke:443, FlinkSQLPaimonExampleF117$4 (flink.debug.paimon)
processElement:54, StreamSink (org.apache.flink.streaming.api.operators)
accept:-1, 1615648663 (org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$2553)
pushToOperator:75, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:50, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:29, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
processElement:-1, SinkConversion$81
accept:-1, 1615648663 (org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$2553)
pushToOperator:75, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:50, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:29, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:51, TimestampedCollector (org.apache.flink.streaming.api.operators)
processLastRowOnChangelog:112, DeduplicateFunctionHelper (org.apache.flink.table.runtime.operators.deduplicate)
processElement:80, ProcTimeDeduplicateKeepLastRowFunction (org.apache.flink.table.runtime.operators.deduplicate)
processElement:32, ProcTimeDeduplicateKeepLastRowFunction (org.apache.flink.table.runtime.operators.deduplicate)
processElement:83, KeyedProcessOperator (org.apache.flink.streaming.api.operators)
lambda$getRecordProcessor$0:60, RecordProcessorUtils (org.apache.flink.streaming.runtime.io)


// 这个类的作用? 消除重复数据; 
// 这属于 table-planner中 StreamExecChangelogNormalize 中代码; 应该是SQL执行后的 转换成 DataStream用的; 
ProcTimeDeduplicateKeepLastRowFunction.processElement(){
	if (inputIsInsertOnly) {
		processLastRowOnProcTime();
	} else {
		processLastRowOnChangelog(); {
			 if (currentKind == RowKind.INSERT || currentKind == RowKind.UPDATE_AFTER) {
				 out.collect(currentRow);
			 }
		}
	}
	 
	 
}
