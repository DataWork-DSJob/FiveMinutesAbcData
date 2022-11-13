
Task, 3��:
	Source: Custom Source
	Map -> Timestamps/Watermarks -> Map 
	Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out 

Task����������߳�: 
	Legacy Source Thread - Source: Custom Source (1/1)#0
	
	Task2: "Map -> Timestamps/Watermarks -> Map"
		Map -> Timestamps/Watermarks -> Map (2/2)#0	
		Map -> Timestamps/Watermarks -> Map (1/2)#0	
	
	Task 3: "Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out"
		Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (2/2)#0	
		Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (1/2)#0	\


������ת������Shuffle ���

	�첽ˢ�����ݵ��߳� OutputFlusher: OutputFlusher for XX
	
		OutputFlusher for Source: Custom Source	
		OutputFlusher for Map -> Timestamps/Watermarks -> Map	 
		OutputFlusher for Map -> Timestamps/Watermarks -> Map	 
		

�ؼ����빦��
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


	
	
	
ԭ���ܽ�
- Taskͨ��RecordWriter�����д��ResultPartition�С�RecordWriter���𽫼�¼�������л���buffer��;
- Task�����뱻����ΪInputGate����InputGate����InputChannel��ɣ�InputChannel�͸�Task��Ҫ���ѵ�ResultSubpartition��һһ��Ӧ�ġ�
-  Taskͨ��ѭ������InputGate.getNextBufferOrEvent() ���������Ĵ�Channel�л�ȡ�������ݣ�������ȡ�����ݽ���������װ�����ӽ��д����⹹����һ��Task�Ļ��������߼�
- SingleInputGate ͨ���ڲ�ά����һ�������γ�һ��������-�����ߵ�ģ�ͣ���InputChannel��������ʱ�ͼ��뵽�����У�����Ҫ��ȡ����ʱ�Ӷ�����ȡ��һ��channel����ȡchannel�е�����
- �����̹߳���ͬһ��Buffer Pool��ͨ��wait()/notifyAll��ͬ����InputGate�����ȡBuffer
	- ��û��Buffer��������ʱ����������sum()��SubTask�̱߳�������ͨ��InputGate�е�inputChannelWithData.wait()����������
	- ����������д�������ݵ�ResultSubPartition����flush��Buffer��, �ỽ����������sum()��SubTask�̣߳�ͨ��inputChannelWithData.notifyAll()�������ѣ�
	- �̱߳����Ѻ���Buffer�ж�ȡ���ݣ��������л��󣬴��ݸ�Operator�е��û������߼�����
	
	



class SingleInputGate {
	int gateIndex;
	Map<IntermediateResultPartitionID, InputChannel> inputChannels;
	InputChannel[] channels;
	PrioritizedDeque<InputChannel> inputChannelsWithData
	BufferPool bufferPool;
	
	//�ؼ�����
	Optional<BufferOrEvent> getNext() 		//Blocking call waiting for next {@link BufferOrEvent}. ������ȡ 
	Optional<BufferOrEvent> pollNext() 		//Poll the {@link BufferOrEvent}.

	InputChannel getChannel(int channelIndex);	//Returns the channel of this gate.
	
	int getNumberOfInputChannels();
	void sendTaskEvent(TaskEvent event) 
	
}



# 1.1 RecordEmit: Output.collect() -> buff.write() -> LocalBufferPool.requestMemorySegment()

// ���һ������ӵ�е�Outputʵ������ RecordWriterOutput , ������.add() ������
/**
* 
 RecordWriterOutput.collect(StreamRecord<OUT> record)
	RecordWriterOutput.emit(T record)
		RecordWriter.emit(T record, int targetSubpartition)
			RecordWriter.serializeRecord()
				SerializationDelegate.write(DataOutputView out)
					StreamElementSerializer.serialize(StreamElement value, DataOutputView target)
						
						typeSerializer.serialize(record.getValue(), target);{
							StringSerializer.serialize() ��ͬ����ͬ���л���ʽ; ��������л��� byte[]��ŵ�DataOutputSerializer.buffer��; 
								StringValue.writeString(){
									for (int i = 0; i < cs.length() +1; i++) {
										int c = cs.charAt(i);
										out.write(c);{// DataOutputSerializer.write()
											this.buffer[this.position++] = (byte) (b & 0xff); д��this�Ļ�����; 
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


/** Task ���߳�, ����ctx.collect(), ͨ�� RecordWriterOutput.collect() ��ڰ�Record�������л������� buffer������; 
*	����߳�:  "Legacy Source Thread - Source: Custom Source (1/1)#0", "Map -> Timestamps/Watermarks -> Map (2/2)#0"
*	

*/

RecordWriterOutput.collect(StreamRecord<OUT> record){
	pushToRecordWriter(record);
		- recordWriter.emit(serializationDelegate);// org.apache.flink.runtime.io.network.api.writer.ChannelSelectorRecordWriter
		- emit(record, channelSelector.selectChannel(record)); {// RecordWriter.emit()
			//1.  serializeRecord(): ��Record���л��� byte[] ������ DataOutputSerializer.buffer ��; 
			ByteBuffer record = serializeRecord(serializer, record); {//RecordWriter.serializeRecord()
				record.write(serializer);//SerializationDelegate
					this.serializer.serialize(this.instance, out);// StreamElementSerializer.serialize()
						typeSerializer.serialize(record.getValue(), target);{
							// Record���ַ������Ͷ���, ���� StringValue ���л�
							StringValue.writeString(record, target);{
								for (int i = 0; i < strlen; i++) {
									int c = cs.charAt(i);
									out.write(c);{// org.apache.flink.core.memory.DataOutputSerializer.write(int b)
										this.buffer[this.position++] = (byte) (b & 0xff);
									}
								}
							}
							// �����Java����, Ĭ�ϲ���Kroy���л�?
							
						}
				return serializer.wrapAsByteBuffer();{
					this.wrapper.position(0);
					this.wrapper.limit(this.position);
					return this.wrapper;// ByteBuffer wrapper, ʵ����: HypeByteBuffer 
				}
			}
			// 2. ��ȡBufferBuilder, û�еĻ��½�, isFull()���˵Ļ�,���ÿ�ˢ��;
			// ��Ҫ���ݽṹ: BufferBuilder[] unicastBufferBuilders: each subpartition maintains a separate BufferBuilder
			targetPartition.emitRecord(record, targetSubpartition);{//BufferWritingResultPartition.emitRecord
				
				// 1. �ҵ����ӷ���/Subpartition �� BufferBuilder������д�����л����(��networkBufferPool�������µı���MemorySegment)
				BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);{
					BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];
					// ��Task��һ�ν���Ϊ��ʱ��,�ᴴ����Ӧ�ӷ���(targetSubpartition)�� BufferBuilder; 
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
					// ��Ӹ�����(byteBuffer) 
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
				
				// 2. Buffer����Record����δд��, �� LocalBufferPool.availableMemorySegments ������µ� buffer/MemorySegment; 
				while (record.hasRemaining()) { ByteBuffer.hasRemaining(){
						return position < limit; // Tells whether there are any elements between the current position and the limit.
					}
					// full buffer, partial record; ��ǰBuffer����, ��Record������δд��; �Ƚ�������buffer; 
					finishUnicastBufferBuilder(targetSubpartition);
					// �ٴ�LocalBufferPool.availableMemorySegments �� request ����һ���µ�MemorySegment; 
					buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition); {//BufferWritingResultPartition
						BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);{//BufferWritingResultPartition
							ensureUnicastMode();
							final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);{//org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition
								// ��LoalBufferPool�ж�ȡ�� �ӷ����� buffer, �÷�����һ�ζ�ȡΪ��,���½� 
								BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);
								if (bufferBuilder != null) {
									return bufferBuilder;
								}
								// ��һ�ζ�ȡ,loalBufferPoolΪ�� 
								bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);{//LocalBufferPool
									return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);{// LocalBufferPool.requestMemorySegmentBlocking()
										// �����ڴ�,һֱ�ȴ� LocalBufferPool �п����buffer/MemSegment��,�����䵽1��;
										// ����ƿ��1: ��� LocalBufferPool �ٳ�δˢ������, ����ͻᷴѹ, ��������; 
										MemorySegment segment; 
										while ((segment = requestMemorySegment(targetChannel)) == null) {
											LocalBufferPool.requestMemorySegment(int targetChannel) {
												// target channel over quota; do not return a segment
												// ���ͨ�����˵� maxBuffersPerChannel(Ĭ��10��, max-buffers-per-channelָ��), �ͻ��򷵻�null��ѭ�������ڴ�, ֹͣ������; 
												if (subpartitionBuffersCount[targetChannel] >= maxBuffersPerChannel) {
													return null;
												}
												// ����Ӷ���ȡ��, �� recycle() �� requestMemorySegmentFromGlobal() ��add()����; 
												segment = availableMemorySegments.poll();
												if (segment == null) { //���ö�������Ϊ��, ��Ȼ�������ڴ� ��������/������; ֱ�� recycle()�� FromGlobal()�� availableMemorySegments.add()
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
				// �����ǰbuffer: BufferBuilder �Ѿ�����; ��� positionMarker ��ǲ��ύ/����ˢ��; 
				// ���� ���ӷ����� unicastBufferBuilders �ÿ�; �Ա������ݴ��� ���� buffer/ MemorySegment; 
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













// OutputFlusher �߳�: "OutputFlusher for Source: Custom Source"


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
		Thread.sleep(timeout);//100 ms, // ��ExecutionOptions���execution.buffer-timeout ��������,Ĭ�� 100ms; 
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
								
								// inputChannelsWithData: PrioritizedDeque<InputChannel>, ���� 
								GateNotificationHelper notification = new GateNotificationHelper(this, inputChannelsWithData);
								
								// when channel is closed, EndOfPartitionEvent is send and a final notification, if EndOfPartitionEvent causes a release, we must ignore the notification
								if (channel.isReleased()) { // LocalInputChannel.isReleased 
									return;
								}
								
								if (!queueChannelUnsafe(channel, priority)) {
									{// SingleInputGate.queueChannelUnsafe()
										inputChannelsWithData.add(channel, priority, alreadyEnqueued);{
											if (!priority) {
												add(element);{ // �� channel: LocalInputChannel ���뵽������; 
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






// �������Ӷ�ȡ Task���, �ͷ� ����; 
// flink-runtime_Input.recycleBuffer()_src1.12
// �߳�: �������ӳɹ���channel�л�ȡbuffer,�ͷ� buff; �߳�: "Window(TumblingEventTimeWindows(3000),"

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
								// ���ж� ��Ӧ�ӷ�����buffer���� �Ƿ��Ѿ��ﵽ maxBuffersPerChannel(12��), ����ﵽ,����ӷ����ݲ�������;
								if (subpartitionBuffersCount[channel]-- == maxBuffersPerChannel) {
									unavailableSubpartitionsCount--;
								}
								// �� availableMemorySegments ���ظ�buffer; 
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
			
			// ���ݵ��ں�, ��ֱ�Ӵ�������?
			if (result.isFullRecord()) {
				processElement(deserializationDelegate.getInstance(), output);
				return InputStatus.MORE_AVAILABLE;
			}
		}
		// ������, currentRecordDeserializer = null ˵��ʲô? 
		Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
		if (bufferOrEvent.get().isBuffer()) {
			processBuffer(bufferOrEvent.get());
		} else {
			processEvent(bufferOrEvent.get());
			return InputStatus.MORE_AVAILABLE;
		}
	}
}






