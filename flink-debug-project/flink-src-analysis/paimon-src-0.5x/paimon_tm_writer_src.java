
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



Paimon 入口核心算子: 
- Sink Writer写出算子: RowDataStoreWriteOperator


// 1. 每条数据进来的时候; 
RowDataStoreWriteOperator.processElement() {
	sinkContext.timestamp = element.hasTimestamp() ? element.getTimestamp() : null;
	SinkRecord record = write.write(new FlinkRowWrapper(element.getValue()));{// StoreSinkWriteImpl
		return write.writeAndReturn(rowData);{//TableWriteImpl.writeAndReturn(InternalRow row)
			SinkRecord record = this.toSinkRecord(row);
			this.write.write(record.partition(), record.bucket(), this.recordExtractor.extract(record)); {// KeyValueFileStoreWrite.write() -> AbstractFileStoreWrite
				// 
				AbstractFileStoreWrite.WriterContainer<T> container = this.getWriterWrapper(partition, bucket);{//AbstractFileStoreWrite.getWriterWrapper
					Map<Integer, AbstractFileStoreWrite.WriterContainer<T>> buckets = (Map)this.writers.get(partition);
					if (buckets == null) {
						buckets = new HashMap();
						this.writers.put(partition.copy(), buckets);
					}
					return (AbstractFileStoreWrite.WriterContainer)((Map)buckets) -> {this.createWriterContainer(partition.copy(), bucket, this.ignorePreviousFiles);};{
						AbstractFileStoreWrite.createWriterContainer(BinaryRow partition, int bucket, boolean ignorePreviousFiles) {
							restoreFiles = this.scanExistingFileMetas(latestSnapshotId, partition, bucket);{
								this.scan.withSnapshot(snapshotId).withPartitionBucket(partition, bucket).plan().files().stream().map(ManifestEntry::file).forEach(existingFileMetas::add);{
									AbstractFileStoreScan.plan(){
										Pair<Snapshot, List<ManifestEntry>> planResult = this.doPlan(this::readManifestFileMeta);
										
									}
								}
							}
							
							RecordWriter<T> writer = this.createWriter(partition.copy(), bucket, (List)restoreFiles, (CommitIncrement)null, this.compactExecutor());
							this.notifyNewWriter(writer);
							return new AbstractFileStoreWrite.WriterContainer(writer, indexMaintainer, latestSnapshotId);
							
						}
					}
				}
				
				// 将数据写都buffer? 
				container.writer.write(data);{//MergeTreeWriter.write(BinaryRow partition, int bucket, T data)
					long sequenceNumber = kv.sequenceNumber() == -1L ? this.newSequenceNumber() : kv.sequenceNumber();
					boolean success = this.writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
					if (!success) {
						this.flushWriteBuffer(false, false);
						success = this.writeBuffer.put(sequenceNumber, kv.valueKind(), kv.key(), kv.value());
						if (!success) {
							throw new RuntimeException("Mem table is too small to hold a single element.");
						}
					}
				}
			}
		}
	}
}


// 2. 执行performCheckpoint() 提交和写出数据落磁盘; 
	StreamTask.performCheckpoint(){
		this.subtaskCheckpointCoordinator.checkpointState() 
			operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());{
				for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators()) {
					operatorWrapper.getStreamOperator().prepareSnapshotPreBarrier(checkpointId); {
						// 带这里是 Paimon的实现代码了
						PrepareCommitOperator.prepareSnapshotPreBarrier(){
							RowDataStoreWriteOperator.prepareCommit();
						}
					}
				}
			}
	}
// Writer实现入口也是 RowDataStoreWriteOperator 
RowDataStoreWriteOperator.prepareCommit();{
	TableWriteImpl.prepareCommit() -> AbstractFileStoreWrite.prepareCommit() {
		Iterator partIter = this.writers.entrySet().iterator();
		while(partIter.hasNext()) {
			Iterator bucketIter = ((Map)partEntry.getValue()).entrySet().iterator();
			while(bucketIter.hasNext()) {
				CommitIncrement increment = writerContainer.writer.prepareCommit(waitCompaction);{
					// 数据刷出,落磁盘 
					MergeTreeWriter.flushWriteBuffer();{
						RollingFileWriter dataWriter = this.writerFactory.createRollingMergeTreeFileWriter(0);
						
						try {
							this.writeBuffer.forEach(this.keyComparator, this.mergeFunction, changelogWriter == null ? null : changelogWriter::write, dataWriter::write); {
								RollingFileWriter.write(T row) {
									if (this.currentWriter == null) { 
										this.openCurrentWriter();{
											KeyValueFileWriterFactory.createDataFileWriter() {
												KeyValueSerializer kvSerializer = new KeyValueSerializer(this.keyType, this.valueType);
												return new KeyValueDataFileWriter(this.fileIO, this.formatContext.writerFactory(level), path, kvSerializer::toRow, this.keyType, this.valueType, this.formatContext.extractor(level), 
															this.schemaId, level, this.formatContext.compression(level), this.options);{
													// StatsCollectingSingleFileWriter -> SingleFileWriter
													this.writer = factory.create(this.out, compression);{// OrcWriterFactory.create()
														return new OrcBulkWriter(this.vectorizer, new WriterImpl((FileSystem)null, unusedPath, opts), out, this.coreOptions.orcWriteBatch());
													}
													
												}
											}
										}
									}
									// 这里是真正 缓存数据写出到磁盘 
									this.currentWriter.write(row);{//KeyValueFileWriterFactory.write(KeyValue kv)
										super.write(kv);{//StatsCollectingSingleFileWriter.write()
											InternalRow rowData = this.writeImpl(record);{//SingleFileWriter.writeImpl()
												InternalRow rowData = (InternalRow)this.converter.apply(record);
												this.writer.addElement(rowData);// FormatWriter.addElement(InternalRow var) {
													// FormatWriter是接口, 其实现类, 默认基于 Orc ?
													OrcBulkWriter.addElement(InternalRow element) {
														this.vectorizer.vectorize(element, this.rowBatch);
													}
													// 还有一个是 Parquet 的写出实现类; 
													ParquetBulkWriter.addElement(InternalRow element);
													
												}
												 ++this.recordCount;
											}
										}
										this.updateMinKey(kv);
										this.updateMaxKey(kv);
										this.updateMinSeqNumber(kv);
									}
									
									++this.recordCount;
									if (this.rollingFile()) {
										this.closeCurrentWriter();
									}
								}
								
							}
						} finally {
							changelogWriter.close();
							dataWriter.close();
						}
						
						this.compactManager.triggerCompaction(forcedFullCompaction);
					}
					
				}
				
				CommitMessageImpl committable = new CommitMessageImpl(partition, bucket, increment.newFilesIncrement(), increment.compactIncrement(), new IndexIncrement((List)newIndexFiles));
				result.add(committable);
				
			}
		}
		
	}
}















