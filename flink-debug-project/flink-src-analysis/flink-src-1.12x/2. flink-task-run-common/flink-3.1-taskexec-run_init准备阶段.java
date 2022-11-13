


//TM.1 TaskManger init 启动和初始化



// Window(TumblingEventTimeWindows(3000), EventTimeTrigger, CoGroupWindowFunction) -> Map -> Filter -> Sink: Print to Std. Err (2/4) 线程: 

Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                beforeInvoke(); // 真正消费数据前,先进行初始化
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();{
                    mailboxProcessor.runMailboxLoop();// 源码细节如下 MailboxProcessor.runMailboxLoop()
                }
                
                // 结束消费和处理后,资源释放;
                afterInvoke();
                
            }
        }
    }
}
 

// 构建 OperatorChain 阶段
isObjectReuseEnabled:720, ExecutionConfig (org.apache.flink.api.common)
createOutputCollector:387, OperatorChain (org.apache.flink.streaming.runtime.tasks)
createChainedOperator:418, OperatorChain (org.apache.flink.streaming.runtime.tasks)
createOutputCollector:354, OperatorChain (org.apache.flink.streaming.runtime.tasks)
createChainedOperator:418, OperatorChain (org.apache.flink.streaming.runtime.tasks)
createOutputCollector:354, OperatorChain (org.apache.flink.streaming.runtime.tasks)
<init>:144, OperatorChain (org.apache.flink.streaming.runtime.tasks)
invoke:393, StreamTask (org.apache.flink.streaming.runtime.tasks)
doRun:705, Task (org.apache.flink.runtime.taskmanager)
run:530, Task (org.apache.flink.runtime.taskmanager)
run:748, Thread (java.lang)


//flink_stream, "Window(xx) 主线程"

StreamTask.invoke(){
	operatorChain = new OperatorChain<>(this, recordWriters);
	headOperator = operatorChain.getHeadOperator();
}


// flink_stream: new OperatorChain() 
new OperatorChain<>(this, recordWriters);{
	Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);
	this.streamOutputs = new RecordWriterOutput<?>[outEdgesInOrder.size()];
	
	this.chainEntryPoint = createOutputCollector(containingTask,configuration,
				chainedConfigs, userCodeClassloader, streamOutputMap, allOps);{
		List<Tuple2<WatermarkGaugeExposingOutput<StreamRecord<T>>, StreamEdge>> allOutputs = new ArrayList<>(4);
		// create collectors for the network outputs 没用输出的就是Network 网络输出的?
		for (StreamEdge outputEdge : operatorConfig.getNonChainedOutputs(userCodeClassloader)) {
			RecordWriterOutput<T> output = (RecordWriterOutput<T>) streamOutputs.get(outputEdge);
			allOutputs.add(new Tuple2<>(output, outputEdge));
		}
		// Create collectors for the chained outputs; 同一Task中,可以chained一起的算子; 创建Output类包装; 
		for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
			WatermarkGaugeExposingOutput<StreamRecord<T>> output = createChainedOperator();{//OperatorChain.createChainedOperator()
				// 递归调用, 
				WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput = createOutputCollector();
				
				// now create the operator and give it the output collector to write its output to; 创建 Operator 
				StreamOperatorFactory<OUT> chainedOperatorFactory = operatorConfig.getStreamOperatorFactory(userCodeClassloader);
				OneInputStreamOperator<IN, OUT> chainedOperator = chainedOperatorFactory.createStreamOperator(containingTask, operatorConfig, chainedOperatorOutput);
				allOperators.add(chainedOperator);
				
				// 对象复用的关键代码: 根据 reuseEnable=true/false, 是否提供CopyingChainingOutput 深拷贝包装类; 默认false 都要深拷贝;
				// objectReuseEnabled 有参数object-reuse-mode控制, 默认=false; 
				if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
					currentOperatorOutput = new ChainingOutput<>(chainedOperator, this, outputTag);
				}
				else {
					TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
					currentOperatorOutput = new CopyingChainingOutput<>(chainedOperator, inSerializer, outputTag, this);
				}
				
				// wrap watermark gauges since registered metrics must be unique
				chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, currentOperatorOutput.getWatermarkGauge()::getValue);
				chainedOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, chainedOperatorOutput.getWatermarkGauge()::getValue);
				
				return currentOperatorOutput;
			}
			allOutputs.add(new Tuple2<>(output, outputEdge));
		}
		
		// if there are multiple outputs, or the outputs are directed, we need to wrap them as one output
		// 变量outputSelectorWrapper 存字节,有数据但序列化返回空 OutputSelector结合;
		List<OutputSelector<T>> selectors = operatorConfig.getOutputSelectors(userCodeClassloader);{
			List<OutputSelector<T>> selectors = InstantiationUtil.readObjectFromConfig(this.config, OUTPUT_SELECTOR_WRAPPER, userCodeClassloader);
			return selectors == null ? Collections.<OutputSelector<T>>emptyList() : selectors;
		}
		
		if (selectors == null || selectors.isEmpty()) {// 接1个输出,简单路径
			// simple path, no selector necessary; 
			if (allOutputs.size() == 1) { // 窗口是,allOutputs.size=0, 为什么? 
				return allOutputs.get(0).f0;
			} else {
				// This is the inverse of creating the normal ChainingOutput.
				// If the chaining output does not copy we need to copy in the broadcast output, otherwise multi-chaining would not work correctly.
				// 和正常 相反(这里是shuffle), 如果没复用的胡
				// 如果启用了 reuseObj, 说明前面对象是可能重用冲突, 这里输出 broadcasting前要copy一下; 
				if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
					return new CopyingBroadcastingOutputCollector<>(asArray, this);
				} else  {
					// 如果没用启用reuseObj,则对象是安全的, 这里之间 broadcast了; 
					return new BroadcastingOutputCollector<>(asArray, this);
				}
			}
		} else {
			// This is the inverse of creating the normal ChainingOutput.
			// If the chaining output does not copy we need to copy in the broadcast output, otherwise multi-chaining would not work correctly.
			if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
				return new CopyingDirectedOutput<>(selectors, allOutputs);
			} else {
				return new DirectedOutput<>(selectors, allOutputs);
			}
		}
		
	}
	
	WatermarkGaugeExposingOutput<StreamRecord<OUT>> output = getChainEntryPoint();
	headOperator = operatorFactory.createStreamOperator(containingTask, configuration, output);
	headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, output.getWatermarkGauge());
	
	this.allOperators = allOps.toArray(new StreamOperator<?>[allOps.size()]);
	success = true;
}


// 1.2 Source 线程的 启动? 以 KafkaSource + FromCollect Source 为例 
SourceStreamTask.LegacySourceFunctionThread.run(){
    headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);{//StreamSource
        run(lockingObject, streamStatusMaintainer, output, operatorChain);{
            this.ctx = StreamSourceContexts.getSourceContext();
            userFunction.run(ctx);{//FlinkKafkaConsumerBase.run()
                
                FlinkKafkaConsumerBase.run(){
                    this.kafkaFetcher = createFetcher();
                    kafkaFetcher.runFetchLoop();{
                        final Handover handover = this.handover;
                        // 启动Kafka消费线程, 持续从Kafka消费数据;
                        consumerThread.start();
                        
                        while (running) {
                            //consumerThread线程拉取的kafka数据存放在handover这个中间容器/缓存中;
                            final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
                            for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {
                                List<ConsumerRecord<byte[], byte[]>> partitionRecords =records.records(partition.getKafkaPartitionHandle());
                                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                    // 这里是处理业务逻辑的核心方法: 
                                    emitRecord(value, partition, record.offset(), record);
                                }
                            }
                            
                        }
                    }
                    
                }
                
                FromElementsFunction.run(){ // 
                    ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
                    // 循环遍历规定数量(numElements)的元素, 这个数是由Driver分配决定?
                    while (isRunning && numElementsEmitted < numElements) {
                        T next = serializer.deserialize(input);
                        // 所谓的collect()就是一层层的往下面传;当需要shuffle或者输出了,就停止;
                        ctx.collect(next);
                        numElementsEmitted++;
                    }
                }
                
            }
        }
    }
    completionFuture.complete(null);
}



// TM.2 Operator处理1条数据的 全流程

// TM2.1 Source, TM算子 轮询 获取 消息/ 待处理事件?

MailboxProcessor.runMailboxLoop(){//MailboxProcessor.runMailboxLoop()
	boolean hasAction = processMail(localMailbox);{//源码中while(processMail(localMailbox)); 没有Mail时会一直阻塞在此,有消息才
		if (!mailbox.createBatch(){// TaskMailboxImpl.createBatch()
			if (!hasNewMail) {
				return !batch.isEmpty();
			}
			
		}) {
			return true;
		}
		
		while (isDefaultActionUnavailable() && isMailboxLoopRunning()) { //循环阻塞在此,等到Mail消息; 有新消息才会进入while()循环中的 runDefaultAction();
			// 阻塞方法, 一直等到直到 queue不为空,取出了一个 headMail:Mail
			mailbox.take(MIN_PRIORITY).run();{//TaskMailboxImpl.take(int priority)
				Mail head = takeOrNull(batch, priority);
				while ((headMail = takeOrNull(queue, priority)) == null) {
					// 接受线程信号; 阻塞在此, 一旦("OutputFlusher for Source")线程发出 notEmpty.signal()信号,就结束等待,处理消息;
					notEmpty.await();
				}
				hasNewMail = !queue.isEmpty(); // 用于 createBatch()中判断;
				return headMail;
			}
		}
		return isMailboxLoopRunning();// return mailboxLoopRunning;
	}
	
	while (hasAction = processMail(localMailbox)) {//阻塞在条件判断的方法中, 判断还处于Running状态时,会进入下面的 runDefaultAction()
		mailboxDefaultAction.runDefaultAction(defaultActionContext); {
			this.processInput();{
				StreamTask.processInput();{
					// 这里不同的 inputProcessor:StreamInputProcessor 实现类,进行不同处理;
					InputStatus status = inputProcessor.processInput();{
						StreamoneInputProcessor.processInput();{}
					}
				}
			
				SourceStreamTask.processInput(controller);{
					
				}
				
				
				
			}
		}
	}
}













