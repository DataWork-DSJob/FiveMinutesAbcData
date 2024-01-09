
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




# 2  RecordEmit: Input.emitNex() Gate.poll(() �¸�������ȡ����? 
// �߳� , Data �ͷ�, ����? 
/**
* StreamTask.processInput()	-> StreamOneInputProcessor.processInput()
	StreamTaskNetworkInput.emitNext()
		// ��NonSpann.segment: MemorySegment �ж�ȡ�ֽ�,�����л��� Object����; 
		result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
			SpillingAdaptiveSpanningRecordDeserializer.getNextRecord() -> readNonSpanningRecord()
				NonSpanningWrapper.readInto()
					NonReusingDeserializationDelegate.read()
						StreamElementSerializer.deserialize() {
							return new StreamRecord<T>(typeSerializer.deserialize(source), timestamp);
								KryoSerializer.deserialize()
									Kryo.readClassAndObject() 
						}
		
		processElement(deserializationDelegate.getInstance(), output); ����һ����ȡ������; 
		
		// ������������, ���ݻ�û����, �ͽ���pollNext() ����һ��MemorySegment ; 
		
		CheckpointedInputGate.pollNext() InputGateWithMetrics.pollNext() 
			SingleInputGate.pollNext() -> getNextBufferOrEvent() -> SingleInputGate.waitAndGetNextData()
				inputChannelOpt = SingleInputGate.getChannel(blocking);
					PrioritizedDeque.poll()
				
				LocalInputChannel.getNextBuffer();
					PipelinedSubpartitionView.getNextBuffer()
						PipelinedSubpartition.pollBuffer()
							buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);
				
*/


// task: "Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (2/2)#0"


// emitNext() 一次就是 接受和向下处理数据一次; 
StreamTaskNetworkInput.emitNext() {
	while (true) {
		// 这里会有2步: 1. 从buffer中拉去;2. 进行反序列化; 
		// 这里的反序列化会很消耗性能; 
		DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
		if (result.isBufferConsumed()) {
			currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
			currentRecordDeserializer = null;
		}
		// 循环getNextRecord(),知道 isFullRecord, 是一个完整数据, 就不需要等待了; 
		if (result.isFullRecord()) {
			// processElement() 真正进入数据处理逻辑; 
			processElement(deserializationDelegate.getInstance(), output);
			return InputStatus.MORE_AVAILABLE;
		}
	}
}


StreamTask.processInput(Controller controller){
	InputStatus status = inputProcessor.processInput();//StreamOneInputProcessor.
	- InputStatus status = input.emitNext(output);{//StreamTaskNetworkInput.emitNext(DataOutput<T> output)
		while (true) {
			// get the stream element from the deserializer
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				if (result.isBufferConsumed()) {
                    currentRecordDeserializer.getCurrentBuffer().recycleBuffer();{//ReadOnlySlicedNetworkBuffer
						getBuffer().recycleBuffer();
						-> release(); -> return this.handleRelease(updater.release(this));
						-> this.deallocate(); // NetworkBuffer
						-> recycler.recycle(memorySegment);{ // LocalBufferPool$SubpartitionBufferRecycler
							bufferPool.recycle(memorySegment, channel); {//LocalBufferPool.recycle()
								while (!notificationResult.isBufferUsed()) {
									
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
			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();{//CheckpointedInputGate.pollNext()
				// pollNext():  Poll the {@link BufferOrEvent}.
				// �����Ĵ�Channel�л�ȡ�������ݣ�������ȡ�����ݽ���������װ�����ӽ��д����⹹����һ��Task�Ļ��������߼���
				Optional<BufferOrEvent> next = inputGate.pollNext();{
					return inputGate.pollNext().map(this::updateMetrics);{//SingleInputGate.pollNext()
						return getNextBufferOrEvent(false);{}{//SingleInputGate.getNextBufferOrEvent()
							Optional<InputWithData<InputChannel, BufferAndAvailability>>  next = waitAndGetNextData(blocking);{//SingleInputGate.waitAndGetNextData()
								 while (true) {
									Optional<InputChannel> inputChannelOpt = getChannel(blocking);
									InputChannel inputChannel = inputChannelOpt.get();
									bufferAndAvailabilityOpt = inputChannel.getNextBuffer();{//LocalInputChannel
										BufferAndBacklog next = subpartitionView.getNextBuffer();{//PipelinedSubpartitionView
											return parent.pollBuffer();{//org.apache.flink.runtime.io.network.partition.PipelinedSubpartition.pollBuffer()
												while (!buffers.isEmpty()) {
													bufferConsumerWithPartialRecordLength = buffers.peek();
													// �������õ�Buffer ?
													BufferConsumer bufferConsumer = bufferConsumerWithPartialRecordLength.getBufferConsumer();
													buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);{
														return buffer.build();{
															return bufferConsumer.build();{//BufferConsumer.build()
																writerPosition.update();
																Buffer slice = buffer.readOnlySlice( currentReaderPosition, 
																	cachedWriterPosition - currentReaderPosition);
																return slice.retainBuffer();
															}
														}
													}
													
													buffer.recycleBuffer();
													
												}
												
												updateStatistics(buffer);
												NetworkActionsLogger.traceOutput("PipelinedSubpartition#pollBuffer", buffer, parent.getOwningTaskName(),subpartitionInfo);
												return new BufferAndBacklog();
											}
										}
										
									}
									
								 }
							}
						}
					}
				}
			}
			if (bufferOrEvent.get().isBuffer()) {
				processBuffer(bufferOrEvent.get());
			} else {
				processEvent(bufferOrEvent.get());
				return InputStatus.MORE_AVAILABLE;
			}
		}
	}
	
}



build:118, BufferConsumer (org.apache.flink.runtime.io.network.buffer)
build:69, BufferConsumerWithPartialRecordLength (org.apache.flink.runtime.io.network.buffer)
buildSliceBuffer:564, PipelinedSubpartition (org.apache.flink.runtime.io.network.partition)
pollBuffer:287, PipelinedSubpartition (org.apache.flink.runtime.io.network.partition)
getNextBuffer:50, PipelinedSubpartitionView (org.apache.flink.runtime.io.network.partition)
getNextBuffer:240, LocalInputChannel (org.apache.flink.runtime.io.network.partition.consumer)
waitAndGetNextData:650, SingleInputGate (org.apache.flink.runtime.io.network.partition.consumer)
getNextBufferOrEvent:625, SingleInputGate (org.apache.flink.runtime.io.network.partition.consumer)
pollNext:611, SingleInputGate (org.apache.flink.runtime.io.network.partition.consumer)
pollNext:109, InputGateWithMetrics (org.apache.flink.runtime.taskmanager)
pollNext:148, CheckpointedInputGate (org.apache.flink.streaming.runtime.io)
emitNext:179, StreamTaskNetworkInput (org.apache.flink.streaming.runtime.io)
processInput:65, StreamOneInputProcessor (org.apache.flink.streaming.runtime.io)
processInput:396, StreamTask (org.apache.flink.streaming.runtime.tasks)
runDefaultAction:-1, 2130263629 (org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$494)
runMailboxLoop:191, MailboxProcessor (org.apache.flink.streaming.runtime.tasks.mailbox)
runMailboxLoop:617, StreamTask (org.apache.flink.streaming.runtime.tasks)
invoke:581, StreamTask (org.apache.flink.streaming.runtime.tasks)
doRun:755, Task (org.apache.flink.runtime.taskmanager)
run:570, Task (org.apache.flink.runtime.taskmanager)
run:748, Thread (java.lang)


// û������ʱ, ������������� 

"Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (2/2)#0" #83 prio=5 os_prio=0 tid=0x00000260ff0dd000 nid=0x3bdc waiting on condition [0x000000bc3b1ff000]
   java.lang.Thread.State: TIMED_WAITING (parking)
        at sun.misc.Unsafe.park(Native Method)
        - parking to wait for  <0x00000006727413b8> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
        at java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:215)
        at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:2163)
        at org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl.take(TaskMailboxImpl.java:149)
        at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.processMail(MailboxProcessor.java:314)
        at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:189)
        at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617)
        at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581)
        at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755)
        at org.apache.flink.runtime.taskmanager.Task.run(Task.java:570)
        at java.lang.Thread.run(Thread.java:748)




# 2.4 ����������Task�߳�: " Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out " 


2022-10-31 21:13:26,482
	�����߳�: 	[thread:  Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (1/2)#0]
	// ������������SubTask: 
	�����ĸ�������: 	taskName		[Source: Custom Source -> Map -> Timestamps/Watermarks -> Map (1/2)#0 (9605f0eb031ec0aba532663a04e2f9e0)] 
	����:				action			PipelinedSubpartition#pollBuffer 
	�����С			buffer.toString()	Buffer{size=13, hash=-1678597810} @ 
	������Ϣ			channelInfo		ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=0}
	
LOG.trace("[{}] {} {} @ {}",
                    taskName,
                    action,
                    buffer.toDebugString(INCLUDE_HASH),
                    channelInfo);



# 2.5 ����������Task�߳�: Map -> Timestamps/Watermarks -> Map 


























// 2. Buffer Pool Mg


setNumBuffers:562, LocalBufferPool (org.apache.flink.runtime.io.network.buffer)
redistributeBuffers:530, NetworkBufferPool (org.apache.flink.runtime.io.network.buffer)
destroyBufferPool:410, NetworkBufferPool (org.apache.flink.runtime.io.network.buffer)
lazyDestroy:544, LocalBufferPool (org.apache.flink.runtime.io.network.buffer)
close:236, ResultPartition (org.apache.flink.runtime.io.network.partition)
closeNetworkResources:960, Task (org.apache.flink.runtime.taskmanager)
run:-1, 595525513 (org.apache.flink.runtime.taskmanager.Task$$Lambda$718)
run:1551, Task$TaskCanceler (org.apache.flink.runtime.taskmanager)
run:748, Thread (java.lang)



org.apache.flink.shaded.netty4.io.netty.channel.nio.SelectedSelectionKeySetSelector.select()	0.0	0.000 ms (0%)	0.000 ms	0.000 ms	0.000 ms








org.apache.flink.runtime.io.network.partition.consumer.GateNotificationHelper.notifyDataAvailable()	0.0017184245	207 ms (0%)	207 ms	207 ms	207 ms




