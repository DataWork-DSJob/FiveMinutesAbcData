
Task, 3个:
	Source: Custom Source
	Map -> Timestamps/Watermarks -> Map 
	Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out 

Task任务计算主线程: 
	Legacy Source Thread - Source: Custom Source (1/1)#0
	
	Task2: "Map -> Timestamps/Watermarks -> Map"
		Map -> Timestamps/Watermarks -> Map (2/2)#0	
		Map -> Timestamps/Watermarks -> Map (1/2)#0	
	
	Task 3: "Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out"
		Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (2/2)#0	
		Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (1/2)#0	\


数据流转和网络Shuffle 相关

	异步刷出数据的线程 OutputFlusher: OutputFlusher for XX
	
		OutputFlusher for Source: Custom Source	
		OutputFlusher for Map -> Timestamps/Watermarks -> Map	 
		OutputFlusher for Map -> Timestamps/Watermarks -> Map	 
		

关键类与功能
	InputGate:	SingleInputGate
	ResultPartition:	IntermediateResultPartitionID, BufferWritingResultPartition, PipelinedResultPartition
	InputChannel:	
		DataOutputSerializer
	LocalBufferPool
	RecordWriter
	GateNotificationHelper
	PipelinedSubpartition
	
	SpillingAdaptiveSpanningRecordDeserializer
	CheckpointedInputGate
	PrioritizedDeque
	LocalInputChannel
	BufferAndBacklog
	PipelinedSubpartition
	BufferConsumer
	PipelinedSubpartition
	BufferConsumerWithPartialRecordLength


	
	
	
原理总结
- Task通过RecordWriter将结果写入ResultPartition中。RecordWriter负责将记录对象序列化到buffer中;
- Task的输入被抽象为InputGate，而InputGate则由InputChannel组成，InputChannel和该Task需要消费的ResultSubpartition是一一对应的。
-  Task通过循环调用InputGate.getNextBufferOrEvent() 方法阻塞的从Channel中获取输入数据，并将获取的数据交给它所封装的算子进行处理，这构成了一个Task的基本运行逻辑
- SingleInputGate 通过内部维护的一个队列形成一个生产者-消费者的模型，当InputChannel中有数据时就加入到队列中，在需要获取数据时从队列中取出一个channel，获取channel中的数据
- 两个线程共享同一个Buffer Pool，通过wait()/notifyAll来同步。InputGate负责读取Buffer
	- 当没有Buffer可以消费时，下游算子sum()的SubTask线程被阻塞（通过InputGate中的inputChannelWithData.wait()方法阻塞）
	- 当上游算子写入结果数据到ResultSubPartition，并flush到Buffer后, 会唤醒下游算子sum()的SubTask线程（通过inputChannelWithData.notifyAll()方法唤醒）
	- 线程被唤醒后会从Buffer中读取数据，经反序列化后，传递给Operator中的用户代码逻辑处理
	
	



class SingleInputGate {
	int gateIndex;
	Map<IntermediateResultPartitionID, InputChannel> inputChannels;
	InputChannel[] channels;
	PrioritizedDeque<InputChannel> inputChannelsWithData
	BufferPool bufferPool;
	
	//关键方法
	Optional<BufferOrEvent> getNext() 		//Blocking call waiting for next {@link BufferOrEvent}. 阻塞获取 
	Optional<BufferOrEvent> pollNext() 		//Poll the {@link BufferOrEvent}.

	InputChannel getChannel(int channelIndex);	//Returns the channel of this gate.
	
	int getNumberOfInputChannels();
	void sendTaskEvent(TaskEvent event) 
	
}



# 1.1 RecordEmit: Output.collect() -> buff.write() -> LocalBufferPool.requestMemorySegment()

// 最后一个算子拥有的Output实现类是 RecordWriterOutput , 调用其.add() 触发了
/**
* 
 RecordWriterOutput.collect(StreamRecord<OUT> record)
	RecordWriterOutput.emit(T record)
		RecordWriter.emit(T record, int targetSubpartition)
			RecordWriter.serializeRecord()
				SerializationDelegate.write(DataOutputView out)
					StreamElementSerializer.serialize(StreamElement value, DataOutputView target)
						
						typeSerializer.serialize(record.getValue(), target);{
							StringSerializer.serialize() 不同对象不同序列化方式; 最后都是序列化成 byte[]后放到DataOutputSerializer.buffer中; 
								StringValue.writeString(){
									for (int i = 0; i < cs.length() +1; i++) {
										int c = cs.charAt(i);
										out.write(c);{// DataOutputSerializer.write()
											this.buffer[this.position++] = (byte) (b & 0xff); 写到this的缓存中; 
										}
									}
								}
						}
						
			BufferWritingResultPartition.emitRecord()
				BufferWritingResultPartition.requestNewBufferBuilderFromPool()
					LocalBufferPool.requestMemorySegment() {
						segment = availableMemorySegments.poll();
						return segment;
					}

*/


/** Task 主线程, 调用ctx.collect(), 通过 RecordWriterOutput.collect() 入口把Record数据序列化并存于 buffer缓存中; 
*	相关线程:  "Legacy Source Thread - Source: Custom Source (1/1)#0", "Map -> Timestamps/Watermarks -> Map (2/2)#0"
*	

*/

RecordWriterOutput.collect(StreamRecord<OUT> record){
	pushToRecordWriter(record);
		- recordWriter.emit(serializationDelegate);// org.apache.flink.runtime.io.network.api.writer.ChannelSelectorRecordWriter
		- emit(record, channelSelector.selectChannel(record)); {// RecordWriter.emit()
			//1.  serializeRecord(): 将Record序列化成 byte[] 并存入 DataOutputSerializer.buffer 中; 
			ByteBuffer record = serializeRecord(serializer, record); {//RecordWriter.serializeRecord()
				record.write(serializer);//SerializationDelegate
					this.serializer.serialize(this.instance, out);// StreamElementSerializer.serialize()
						typeSerializer.serialize(record.getValue(), target);{
							// Record是字符串类型对象, 采用 StringValue 序列化
							StringValue.writeString(record, target);{
								for (int i = 0; i < strlen; i++) {
									int c = cs.charAt(i);
									out.write(c);{// org.apache.flink.core.memory.DataOutputSerializer.write(int b)
										this.buffer[this.position++] = (byte) (b & 0xff);
									}
								}
							}
							// 如果是Java对象, 默认采用Kroy序列化?
							
						}
				return serializer.wrapAsByteBuffer();{
					this.wrapper.position(0);
					this.wrapper.limit(this.position);
					return this.wrapper;// ByteBuffer wrapper, 实现类: HypeByteBuffer 
				}
			}
			// 2. 获取BufferBuilder, 没有的话新建, isFull()满了的话,就置空刷新;
			// 主要数据结构: BufferBuilder[] unicastBufferBuilders: each subpartition maintains a separate BufferBuilder
			targetPartition.emitRecord(record, targetSubpartition);{//BufferWritingResultPartition.emitRecord
				
				// 1. 找到该子分区/Subpartition 的 BufferBuilder，用于写入序列化结果(在networkBufferPool中申请新的本地MemorySegment)
				BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);{
					BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];
					// 该Task第一次进入为空时会,会创建相应子分区(targetSubpartition)的 BufferBuilder; 
					if (buffer == null) {
						buffer = requestNewUnicastBufferBuilder(targetSubpartition); {
							BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);{//BufferWritingResultPartition.requestNewBufferBuilderFromPool()
								BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);{//LocalBufferPool.
									return toBufferBuilder(requestMemorySegment(targetChannel), targetChannel);{//LocalBufferPool.requestMemorySegment()
										segment = availableMemorySegments.poll();
										checkConsistentAvailability();
										return segment;
									}
								}
								return bufferBuilder;
							}
							unicastBufferBuilders[targetSubpartition] = bufferBuilder;
						}
						subpartitions[targetSubpartition].add(buffer.createBufferConsumerFromBeginning(), 0);
					}
					// 添加该数据(byteBuffer) 
					buffer.appendAndCommit(record);{
						int writtenBytes = append(source);{
							int needed = source.remaining();
							int toCopy = Math.min(needed, available);
							memorySegment.put(positionMarker.getCached(), source, toCopy);
							return toCopy;
						}
						commit();{
							positionMarker.commit();
						}
						return writtenBytes
					}
					return buffer;
				}
				
				// 2. Buffer已满Record数据未写完, 从 LocalBufferPool.availableMemorySegments 申请个新的 buffer/MemorySegment; 
				while (record.hasRemaining()) { ByteBuffer.hasRemaining(){
						return position < limit; // Tells whether there are any elements between the current position and the limit.
					}
					// full buffer, partial record; 当前Buffer已满, 而Record数据尚未写完; 先结束保存buffer; 
					finishUnicastBufferBuilder(targetSubpartition);
					// 再从LocalBufferPool.availableMemorySegments 中 request 申请一个新的MemorySegment; 
					buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition); {//BufferWritingResultPartition
						BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);{//BufferWritingResultPartition
							ensureUnicastMode();
							final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);{//org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition
								// 从LoalBufferPool中读取该 子分区的 buffer, 该分区第一次读取为空,会新建 
								BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
								if (bufferBuilder != null) {
									return bufferBuilder;
								}
								// 第一次读取,loalBufferPool为空 
								bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);{//LocalBufferPool
									return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);{// LocalBufferPool.requestMemorySegmentBlocking()
										// 阻塞在此,一直等待 LocalBufferPool 有空余的buffer/MemSegment了,并分配到1个;
										// 性能瓶颈1: 如果 LocalBufferPool 迟迟未刷出空余, 这里就会反压, 降低性能; 
										MemorySegment segment; 
										while ((segment = requestMemorySegment(targetChannel)) == null) {
											LocalBufferPool.requestMemorySegment(int targetChannel) {
												// target channel over quota; do not return a segment
												// 如果通道满了到 maxBuffersPerChannel(默认10个, max-buffers-per-channel指定), 就会因返回null而循环阻塞在此, 停止发数据; 
												if (subpartitionBuffersCount[targetChannel] >= maxBuffersPerChannel) {
													return null;
												}
												// 这里从队列取出, 在 recycle() 或 requestMemorySegmentFromGlobal() 中add()回来; 
												segment = availableMemorySegments.poll();
												if (segment == null) { //可用队列中若为空, 依然会阻塞在此 不发数据/不计算; 直到 recycle()或 FromGlobal()中 availableMemorySegments.add()
													return null;
												}
												
												if (++subpartitionBuffersCount[targetChannel] == maxBuffersPerChannel) {
													unavailableSubpartitionsCount++;
												}
												return segment;
											}
											getAvailableFuture().get();
										}
										return segment;
									}
								}
								return bufferBuilder;
							}
							unicastBufferBuilders[targetSubpartition] = bufferBuilder;
						}
						final int partialRecordBytes = buffer.appendAndCommit(remainingRecordBytes);
						subpartitions[targetSubpartition].add( buffer.createBufferConsumerFromBeginning(), partialRecordBytes);
						return buffer;
					}
				}
				// 如果当前buffer: BufferBuilder 已经满了; 则把 positionMarker 标记并提交/提醒刷新; 
				// 并把 该子分区的 unicastBufferBuilders 置空; 以便新数据从新 申请 buffer/ MemorySegment; 
				if (buffer.isFull()) { // return positionMarker.getCached() == getMaxCapacity();
					finishUnicastBufferBuilder(targetSubpartition);{//BufferWritingResultPartition
						if (bufferBuilder != null) {
							int writtenBytes = bufferBuilder.finish();{
								int writtenBytes = positionMarker.markFinished();
								commit();
								return writtenBytes;
							}
							numBytesOut.inc(writtenBytes);
							numBuffersOut.inc();
							unicastBufferBuilders[targetSubpartition] = null;
						}
					}
				}
				
			}
			
		}
}













// OutputFlusher 线程: "OutputFlusher for Source: Custom Source"


# 1.2 RecordEmit:  flushAll() -> notifyChannelNonEmpty() -> notifyDataAvailable()

/**
*	RecordWriter$OutputFlusher.run() -> RecordWriter.flushAll()	
		PipelinedResultPartition.flushAll()
			BufferWritingResultPartition.flushAllSubpartitions()
				PipelinedSubpartition.flush() -> notifyDataAvailable()
					PipelinedSubpartitionView.notifyDataAvailable() -> LocalInputChannel.notifyDataAvailable()
						InputChannel.notifyChannelNonEmpty()
							SingleInputGate.queueChannel()
								GateNotificationHelper.notifyDataAvailable(){
									availabilityMonitor.notifyAll();
									toNotify = inputGate.availabilityHelper.getUnavailableToResetAvailable();
								}
							
					
*/

RecordWriter$OutputFlusher.run(){
	while (running) {
		Thread.sleep(timeout);//100 ms, // 由ExecutionOptions类的execution.buffer-timeout 参数控制,默认 100ms; 
		flushAll();
			- targetPartition.flushAll();//PipelinedResultPartition
			- flushAllSubpartitions(false);{
				for (ResultSubpartition subpartition : subpartitions) {
					subpartition.flush();{//PipelinedSubpartition.flush()
						
						// if there is more then 1 buffer, we already notified the reader (at the latest when adding the second buffer)
						notifyDataAvailable = !isBlocked
                            && buffers.size() == 1
                            && buffers.peek().getBufferConsumer().isDataAvailable();
						
						if (notifyDataAvailable) {
							notifyDataAvailable();//PipelinedSubpartition.
							- readView.notifyDataAvailable(); //PipelinedSubpartitionView.notifyDataAvailable()
							- availabilityListener.notifyDataAvailable();//LocalInputChannel.notifyDataAvailable()
							- notifyChannelNonEmpty();// InputChannel.
							- inputGate.notifyChannelNonEmpty(this);//SingleInputGate
							- queueChannel(checkNotNull(channel), null);{//SingleInputGate.queueChannel()
								
								// inputChannelsWithData: PrioritizedDeque<InputChannel>, 队列 
								GateNotificationHelper notification = new GateNotificationHelper(this, inputChannelsWithData);
								
								// when channel is closed, EndOfPartitionEvent is send and a final notification, if EndOfPartitionEvent causes a release, we must ignore the notification
								if (channel.isReleased()) { // LocalInputChannel.isReleased 
									return;
								}
								
								if (!queueChannelUnsafe(channel, priority)) {
									{// SingleInputGate.queueChannelUnsafe()
										inputChannelsWithData.add(channel, priority, alreadyEnqueued);{
											if (!priority) {
												add(element);{ // 讲 channel: LocalInputChannel 加入到队列中; 
													deque.add(element);
												}
											}
										}
										return true;
									}
									return;
								}
								
								
								if (priority && inputChannelsWithData.getNumPriorityElements() == 1) {
									notification.notifyPriority();{//GateNotificationHelper
										toNotifyPriority = inputGate.priorityAvailabilityHelper.getUnavailableToResetAvailable();
									}
								}
								if (inputChannelsWithData.size() == 1) {
									notification.notifyDataAvailable();{//GateNotificationHelper.notifyDataAvailable()
										availabilityMonitor.notifyAll();
										toNotify = inputGate.availabilityHelper.getUnavailableToResetAvailable();
									}
								}
								
							}
							
						}
					}
				}
			}
	}
}






// 下游算子读取 Task完毕, 释放 缓存; 
// flink-runtime_Input.recycleBuffer()_src1.12
// 线程: 下游算子成功从channel中获取buffer,释放 buff; 线程: "Window(TumblingEventTimeWindows(3000),"

StreamTaskNetworkInput.emitNext(DataOutput<T> output){
	while (true) {
		// get the stream element from the deserializer
		if (currentRecordDeserializer != null) {
			DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
			
			if (result.isBufferConsumed()) { // return this.isBufferConsumed;
				currentRecordDeserializer.getCurrentBuffer().recycleBuffer();{//ReadOnlySlicedNetworkBuffer
					getBuffer().recycleBuffer();
					-> release(); -> return this.handleRelease(updater.release(this));
					-> this.deallocate(); // NetworkBuffer
					-> recycler.recycle(memorySegment);{ // LocalBufferPool$SubpartitionBufferRecycler
						bufferPool.recycle(memorySegment, channel); {//LocalBufferPool.recycle()
							while (!notificationResult.isBufferUsed()) {
								// 先判断 相应子分区的buffer数量 是否已经达到 maxBuffersPerChannel(12个), 如果达到,则该子分区暂不可用了;
								if (subpartitionBuffersCount[channel]-- == maxBuffersPerChannel) {
									unavailableSubpartitionsCount--;
								}
								// 往 availableMemorySegments 里归回该buffer; 
								if (isDestroyed || hasExcessBuffers()) {
									returnMemorySegment(segment);
									return;
								} else {
									availableMemorySegments.add(segment);
									if (!availabilityHelper.isApproximatelyAvailable()
											&& unavailableSubpartitionsCount == 0) {
										toNotify = availabilityHelper.getUnavailableToResetAvailable();
									}
									break;
								}
							}
						}
					}
					bufferPool.recycle(memorySegment, channel);
					
					{//NetworkBuffer
						release(); -> return this.handleRelease(updater.release(this));{//AbstractReferenceCountedByteBuf
							this.deallocate();
						}
					}
				}
				currentRecordDeserializer = null;
			}
			
			// 数据到期后, 就直接处理数据?
			if (result.isFullRecord()) {
				processElement(deserializationDelegate.getInstance(), output);
				return InputStatus.MORE_AVAILABLE;
			}
		}
		// 到这里, currentRecordDeserializer = null 说明什么? 
		Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
		if (bufferOrEvent.get().isBuffer()) {
			processBuffer(bufferOrEvent.get());
		} else {
			processEvent(bufferOrEvent.get());
			return InputStatus.MORE_AVAILABLE;
		}
	}
}






