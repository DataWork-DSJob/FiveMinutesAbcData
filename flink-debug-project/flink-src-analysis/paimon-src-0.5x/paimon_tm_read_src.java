
Paimon 入口核心算子: 
- Sink Writer写出算子: RowDataStoreWriteOperator
- 

paimon RecordReader 接口, 有9个实现类 
	//core, io下 
	RowDataFileRecordReader.readBatch();
	KeyValueDataFileRecordReader.readBatch();
	// core, mergtree包下
	DropDeleteReader.readBatch();
	ConcatRecordReader.readBatch();
	SortMergeReader 接口 
	// table.source下
	KeyValueTableRead.RowDataFileRecordReader
	// format3个包分别 avro,orc,parquet, 默认基于Orc: OrcVectorizedReader
	AvroReader
	OrcReaderFactory.OrcVectorizedReader.readBatch();
	ParquetReaderFactory.ParquetReader.readBatch();



// 1. Task进程中 处理事件的线程,上游线程; 

SourceOperator.handleOperatorEvent(OperatorEvent event){
	if (event instanceof WatermarkAlignmentEvent) {
		updateMaxDesiredWatermark((WatermarkAlignmentEvent) event);
		checkWatermarkAlignment();
		checkSplitWatermarkAlignment();
	} else if (event instanceof AddSplitEvent) {
		handleAddSplitsEvent(((AddSplitEvent<SplitT>) event));{
			List<SplitT> newSplits = event.splits(splitSerializer);
			sourceReader.addSplits(newSplits);{
				SplitFetcher<E, SplitT> fetcher = getRunningFetcher();
				if (fetcher == null) {
					fetcher = createSplitFetcher();
					// Add the splits to the fetchers.
					fetcher.addSplits(splitsToAdd);
					startFetcher(fetcher);{//SplitFetcherManager.startFetcher
						// Executor提交执行 fetch命令(事件)
						executors.submit(fetcher); {
							SplitFetcher.run();{
								while (runOnce());{//SplitFetcher.runOnce
									 taskFinished = task.run();{//FetchTask.run()
										if (!isWakenUp() && lastRecords == null) {
											lastRecords = splitReader.fetch();
										}
										
										return true;
									 }
								}
							}
							
						}
					}
				} else {
					fetcher.addSplits(splitsToAdd);
				}
			}
		}
	} else if (event instanceof SourceEventWrapper) {
		sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
	} else if (event instanceof NoMoreSplitsEvent) {
		sourceReader.notifyNoMoreSplits();
	} else {
		throw new IllegalStateException("Received unexpected operator event " + event);
	}
}


// 2. Task进程中, 实际执行读取数据的线程:  RecordReader.readBatch(), 默认基于Orc: OrcVectorizedReader 
	readBatch:220, OrcReaderFactory$OrcVectorizedReader (org.apache.paimon.format.orc)
	readBatch:65, KeyValueDataFileRecordReader (org.apache.paimon.io)
	readBatch:68, KeyValueTableRead$RowDataRecordReader (org.apache.paimon.table.source)
	fetch:103, FileStoreSourceSplitReader (org.apache.paimon.flink.source)


SplitFetcher.run();{
	while (runOnce());{//SplitFetcher.runOnce
		 taskFinished = task.run();{//FetchTask.run()
			if (!isWakenUp() && lastRecords == null) {
				lastRecords = splitReader.fetch();{//FileStoreSourceSplitReader.fetch()
					FileStoreRecordIterator iterator = pool();
					RecordIterator<InternalRow> nextBatch;
					if (currentFirstBatch != null) {
						nextBatch = currentFirstBatch;
						currentFirstBatch = null;
					} else {
						nextBatch = reachLimit()? null : Objects.requireNonNull(currentReader).recordReader().readBatch();{//KeyValueTableRead$RowDataRecordReader.readBatch()
							RecordIterator<KeyValue> batch = this.wrapped.readBatch();{//KeyValueDataFileRecordReader.readBatch()
								RecordIterator<InternalRow> iterator = this.reader.readBatch();{//OrcReaderFactory$OrcVectorizedReader.readBatch()
									// RecordReader 接口, 有9个实现类 
									//core, io下 
									RowDataFileRecordReader.readBatch();
									KeyValueDataFileRecordReader.readBatch();
									// core, mergtree包下
									DropDeleteReader.readBatch();
									ConcatRecordReader.readBatch();
									SortMergeReader 接口 
									// table.source下
									KeyValueTableRead.RowDataFileRecordReader
									// format3个包分别 avro,orc,parquet
									AvroReader
									OrcReaderFactory.OrcVectorizedReader.readBatch();
									ParquetReaderFactory.ParquetReader.readBatch();
									
								}
								return iterator == null ? null : new KeyValueDataFileRecordReader.KeyValueDataFileRecordIterator(iterator, this.indexMapping, this.castMapping);
							}
							return batch == null ? null : KeyValueTableRead.this.rowDataRecordIteratorFromKv(batch);
						}
					}
					// 将迭代器 干嘛? 
					return FlinkRecordsWithSplitIds.forRecords(currentSplitId, iterator.replace(nextBatch));
				}
			}
			
			return true;
		 }
	}
}



readBatch:220, OrcReaderFactory$OrcVectorizedReader (org.apache.paimon.format.orc)
readBatch:65, KeyValueDataFileRecordReader (org.apache.paimon.io)
readBatch:68, KeyValueTableRead$RowDataRecordReader (org.apache.paimon.table.source)
fetch:103, FileStoreSourceSplitReader (org.apache.paimon.flink.source)





// 3. 关于 Orc Reader 

class KeyValueDataFileRecordReader {
	// 各种格式Reader的 
	org.apache.paimon.reader.RecordReader<InternalRow> reader; 
		class OrcReaderFactory$OrcVectorizedReader {
			org.apache.orc.RecordReader orcReader;
				class RecordReaderImpl {
					Path path;// /tmp/paimon_f117/default.db/dws_trade_summary_10s/bucket-0/data-b36b47f1-d262-41e4-b051-7bafa0fc6589-0.orc
					List<StripeInformation> stripes = new ArrayList();
					StripeFooter stripeFooter;
					TypeDescription schema;
					TreeReader reader;
					RowIndex[] indexes;
					DataReader dataReader;
				}
			Pool<OrcReaderFactory.OrcReaderBatch> pool;
		}
	
	KeyValueSerializer serializer;
	
	int[] indexMapping;
	CastFieldGetter[] castMapping;
}

OrcReaderFactory$OrcVectorizedReader {
	RecordReader orcReader;
	
}


















