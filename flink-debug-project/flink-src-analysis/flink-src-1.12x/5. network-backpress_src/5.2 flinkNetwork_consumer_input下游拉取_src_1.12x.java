
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




# 2  RecordEmit: Input.emitNex() Gate.poll(() 下个算子拉取缓存? 
// 线程 , Data 释放, 可用? 
/**
* StreamTask.processInput()	-> StreamOneInputProcessor.processInput()
	StreamTaskNetworkInput.emitNext()
		// 从NonSpann.segment: MemorySegment 中读取字节,并序列化成 Object对象; 
		result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
			SpillingAdaptiveSpanningRecordDeserializer.getNextRecord() -> readNonSpanningRecord()
				NonSpanningWrapper.readInto()
					NonReusingDeserializationDelegate.read()
						StreamElementSerializer.deserialize() {
							return new StreamRecord<T>(typeSerializer.deserialize(source), timestamp);
								KryoSerializer.deserialize()
									Kryo.readClassAndObject() 
						}
		
		processElement(deserializationDelegate.getInstance(), output); 处理一条拉取的数据; 
		
		// 不是完整对象, 数据还没拉完, 就进行pollNext() 拉下一个MemorySegment ; 
		
		CheckpointedInputGate.pollNext() InputGateWithMetrics.pollNext() 
			SingleInputGate.pollNext() -> getNextBufferOrEvent() -> SingleInputGate.waitAndGetNextData()
				inputChannelOpt = SingleInputGate.getChannel(blocking);
					PrioritizedDeque.poll()
				
				LocalInputChannel.getNextBuffer();
					PipelinedSubpartitionView.getNextBuffer()
						PipelinedSubpartition.pollBuffer()
							buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);
				
*/


// 线程,第二个task: "Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (2/2)#0"

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
				
				// 数据到期后, 就直接处理数据?
				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);
					return InputStatus.MORE_AVAILABLE;
				}
			}
			// 到这里, currentRecordDeserializer = null 说明什么? 
			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();{//CheckpointedInputGate.pollNext()
				// pollNext():  Poll the {@link BufferOrEvent}.
				// 阻塞的从Channel中获取输入数据，并将获取的数据交给它所封装的算子进行处理，这构成了一个Task的基本运行逻辑。
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
													// 消费所用的Buffer ?
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


// 没有数据时, 是阻塞在这里的 

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




# 2.4 下游算子主Task线程: " Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out " 


2022-10-31 21:13:26,482
	运行线程: 	[thread:  Window(TumblingEventTimeWindows(3000), EventTimeTrigger, MyJsonProcWindow) -> Map -> Sink: Print to Std. Out (1/2)#0]
	// 接受数据来自SubTask: 
	来自哪个子任务: 	taskName		[Source: Custom Source -> Map -> Timestamps/Watermarks -> Map (1/2)#0 (9605f0eb031ec0aba532663a04e2f9e0)] 
	动作:				action			PipelinedSubpartition#pollBuffer 
	缓存大小			buffer.toString()	Buffer{size=13, hash=-1678597810} @ 
	渠道信息			channelInfo		ResultSubpartitionInfo{partitionIdx=0, subPartitionIdx=0}
	
LOG.trace("[{}] {} {} @ {}",
                    taskName,
                    action,
                    buffer.toDebugString(INCLUDE_HASH),
                    channelInfo);



# 2.5 下游算子主Task线程: Map -> Timestamps/Watermarks -> Map 


























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




