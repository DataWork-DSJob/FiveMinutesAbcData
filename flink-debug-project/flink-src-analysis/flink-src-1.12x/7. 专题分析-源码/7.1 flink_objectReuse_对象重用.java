

// flink_1.12_src 
Task.run(){
	doRun();{
		// 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
		// StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
		invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
			StreamTask.invoke(){
				beforeInvoke(); { // 真正消费数据前,先进行初始化
					
					// flink_stream: new OperatorChain() 
					operatorChain =new OperatorChain<>(this, recordWriters);{
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
								// Recursively create chain of operators that starts from the given . 递归创建Chain 
								WatermarkGaugeExposingOutput<StreamRecord<T>> output = createOperatorChain(containingTask,allOperatorWrappers);{//OperatorChain.createOperatorChain()
									// create the output that the operator writes to first. this may recursively create more
									// 递归, 
									WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput = createOutputCollector();
									OneInputStreamOperator<IN, OUT> chainedOperator = createOperator();
									// 递归完了, 这里创建和包装具体的 算子对象, 就是跟进是否 objectReuse复用,创建 ChainingOutput 还是深拷贝的 CopyingChainingOutput
									return wrapOperatorIntoOutput(chainedOperator, containingTask, operatorConfig, userCodeClassloader, outputTag); {
										if (containingTask.getExecutionConfig().isObjectReuseEnabled()) {
											currentOperatorOutput = new ChainingOutput<>(operator, this, outputTag);
										} else {
											TypeSerializer<IN> inSerializer = operatorConfig.getTypeSerializerIn1(userCodeClassloader);
											currentOperatorOutput = new CopyingChainingOutput<>(operator, inSerializer, outputTag, this);
										}
										return currentOperatorOutput;
									}
								}
								allOutputs.add(new Tuple2<>(output, outputEdge));
							}
							
							// if there are multiple outputs, or the outputs are directed, we need to wrap them as one output
							// 变量outputSelectorWrapper 存字节,有数据但序列化返回空 OutputSelector结合;
							List<OutputSelector<T>> selectors = operatorConfig.getOutputSelectors(userCodeClassloader);{
								List<OutputSelector<T>> selectors = InstantiationUtil.readObjectFromConfig(this.config, OUTPUT_SELECTOR_WRAPPER, userCodeClassloader);
								return selectors == null ? Collections.<OutputSelector<T>>emptyList() : selectors;
							}
							
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
							
						}
						
						success = true;
					}

					headOperator = operatorChain.getHeadOperator();
				}
				// 在这里里面循环接受消息,并运行;
				runMailboxLoop();
				// 结束消费和处理后,资源释放;
				afterInvoke();
			}
		}
	}
}



算子调用StackTrace: 
	com.eoi.jax.flink.job.process.rectransf.RecordTransformJobFunction.processElement(Object, ProcessFunction$Context, Collector)

	org.apache.flink.streaming.api.operators.ProcessOperator.processElement(StreamRecord)
	org.apache.flink.streaming.runtime.tasks.OperatorChain$ChainingOutput.pushToOperator(StreamRecord)
	org.apache.flink.streaming.runtime.tasks.OperatorChain$ChainingOutput.collect(StreamRecord)
	org.apache.flink.streaming.runtime.tasks.OperatorChain$ChainingOutput.collect(Object)
	org.apache.flink.streaming.api.operators.AbstractStreamOperator$CountingOutput.collect(StreamRecord)
	org.apache.flink.streaming.api.operators.AbstractStreamOperator$CountingOutput.collect(Object)
	org.apache.flink.streaming.api.operators.StreamSourceContexts$ManualWatermarkContext.processAndCollect(Object)

	// Source 算子: StressTestFunction 
	org.apache.flink.streaming.api.operators.StreamSourceContexts$WatermarkContext.collect(Object)
	com.eoi.jax.flink.job.source.stresstest.impl.EnableReuseStFunc.produce(SourceFunction$SourceContext)






// ChainingOutput 将对象直接传给下游算子处理, 不做任何处理;
	ChainingOutput.collect(StreamRecord<T> record) {
		if (this.outputTag != null) {
			return;
		}
		pushToOperator(record); {//ChainingOutput.pushToOperator()
			StreamRecord<T> castRecord = (StreamRecord<T>) record;
			input.processElement(castRecord);
		}
	}


// CopyingChainingOutput 深拷贝输出类: 为每个Map新建对象并递归遍历其每个字段, 新建一个StreamRecord对象;

	CopyingChainingOutput.collect(StreamRecord<T> record) {
		if (this.outputTag != null) {
			// we are not responsible for emitting to the main output.
			return;
		}
		pushToOperator(record); {// CopyingChainingOutput.pushToOperator()
			StreamRecord<T> castRecord = (StreamRecord<T>) record;
			T valueCopy = serializer.copy(castRecord.getValue());{// KryoSerializer.copy(T from)
				checkKryoInitialized();
				return kryo.copy(from);{// com.esotericsoftware.kryo.Kryo.copy()
					// 如果实现了 KryoCopyable.copy() 接口, 直接调copy()
					if (object instanceof KryoCopyable) {
						copy = ((KryoCopyable)object).copy(this);
					} else {
						// 先获取该数据类型的序列器(这里是 MapSerializer)
						Serializer serializer = getSerializer(object.getClass());{
							return getRegistration(type).getSerializer(); {//Kryo.getRegistration(type)
								Registration registration = classResolver.getRegistration(type);
							}
						}
						copy = serializer.copy(this, object);{
							//外层对象是 MapSerializer, Map序列化 要先创建Map对象, 逐个字段依次copy对象, 
							MapSerializer.copy(Kryo kryo, Map original) {
								Map copy = createCopy(kryo, original);{
									return kryo.newInstance(original.getClass()); {
										Registration registration = getRegistration(type);
										ObjectInstantiator instantiator = registration.getInstantiator();
										if (instantiator == null) {
											instantiator = newInstantiator(type);
											registration.setInstantiator(instantiator);
										}
										return (T)instantiator.newInstance();  // new JSONObject() new HashMap();
									}
								}
								for (Iterator iter = original.entrySet().iterator(); iter.hasNext();) {
									Entry entry = (Entry)iter.next();
									// 每个Key都要copy, 每个value也都要copy; 
									copy.put(kryo.copy(entry.getKey()), kryo.copy(entry.getValue()));
								}
								return copy;
							}
							
							// Int, Double, Long, String 字符串等 基本类型对象的拷贝, 就是直接返回;
							StringSerializer.copy() {
								if (immutable) return original;
							}
							DoubleSerializer.copy() {
								if (immutable) return original;
							}
							ClassSerializer.copy() {}
						}
					}
				}
			}
			StreamRecord<T> copy = castRecord.copy(valueCopy); {//StreamRecord.copy(valueCopy)
				// 新建1个StreamRecord对象,重新包装;
				StreamRecord<T> copy = new StreamRecord<>(valueCopy);{
					this.value = value;
				}
				copy.timestamp = this.timestamp;
				return copy;
			}
			input.setKeyContextElement(copy);
			input.processElement(copy);
		}
	}





// flink_1.12_src: side_output 
/** 
* 对象重用情况下的, 测流输出的源码; 
* 测流输出时, 通过Collect.out(), Context.output() 2个不同接口往后输出; 
* 输出对象,即下游接收对象, 故而会发生同一对象不安全问题; 
* 
*/


ProcessFunction.processElement(IN value, Context ctx, Collector<OUT> out) {
	// 一般是带时间戳的输出; 主输出通道不会处理(往后传送)测流数据; 
	out.collect(value);{//TimestampedCollector.collect()
		// 转换成StreamRecord对象, 这里替换下值, 不新建StreamRecord;
		StreamRecord<OUT> streamRecord = reuse.replace(record);{//StreamRecord.replace(element)
			this.value = (T) element;
			return (StreamRecord<X>) this;
		}
		output.collect(streamRecord);{//CountingOutput.collect()
			// CopyingBroadcasting:  performs a shallow copy of the StreamRecord to ensure that multi-chaining works correctly
			// 会进行浅copy:  仅仅把StreamRecord包装对象换一下; 
			output.collect(record);{//CopyingBroadcastingOutputCollector.collect()
				// 这里最后一个output不遍历, 因为最后一个 OutputTag的, 其通过ctx.output(outSide, value) 往后走;
				for (int i = 0; i < outputs.length - 1; i++) {
					Output<StreamRecord<T>> output = outputs[i];
					// Creates a copy of this stream record. Uses the copied value as the value for the new record, i.e., only copies timestamp fields.
					// 包装(对象)换一个, T:value 对象不变, 换汤不换药; 
					StreamRecord<T> shallowCopy = record.copy(record.getValue());{//StreamRecord.copy(T valueCopy)
						StreamRecord<T> copy = new StreamRecord<>(valueCopy);
						copy.timestamp = this.timestamp;
						copy.hasTimestamp = this.hasTimestamp;
						return copy;
					}
					
					// 这里向下游输出; 
					output.collect(shallowCopy);{//ChainingOutput.collect()
						pushToOperator(record);
							input.processElement(castRecord);
								MyMapFunc.map();
					}
				}
				
				// 这个 CopyingBroadcastingOutputCollector挺奇怪的; 因为有 outputTag不为空 就又不输出了; 
				// don't copy for the last output
				outputs[outputs.length - 1].collect(record);{//ChainingOutput
					if (this.outputTag != null) {
						// we are not responsible for emitting to the main output.
						return;
					}
				}
				
			}
		}
	}
	
	// 测流数据, 走这边; 
	ctx.output(outSide, value);{ // ProcessOperator$ContextImpl.output()
		if (outputTag == null) { throw new IllegalArgumentException("OutputTag must not be null.");}
		output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp())); {
			// 注意这个是带OutputTag入参的接口,  Output.collect(outputTag, record)
			output.collect(outputTag, record);{//CopyingBroadcastingOutputCollector.collect(OutputTag<X> outputTag, StreamRecord<X> record)
				// 对于 OutputTag, 这个for里不会往后输出; 
				for (int i = 0; i < outputs.length - 1; i++) {
					Output<StreamRecord<T>> output = outputs[i];
					StreamRecord<X> shallowCopy = record.copy(record.getValue());
					output.collect(outputTag, shallowCopy);{//ChainingOutput.collect(outputTag, record)
						// 若该 output.outputTag 为空或者不等于 传入得tag,则不输出;
						if (OutputTag.isResponsibleFor(this.outputTag, outputTag){
							return other.equals(owner);
						}) {
							pushToOperator(record);
						}
					}
				}
				// OutputTag测流, 要在这里输出; 
				if (outputs.length > 0) {
					// don't copy for the last output
					outputs[outputs.length - 1].collect(outputTag, record);{
						if (OutputTag.isResponsibleFor(this.outputTag, outputTag)) {
							pushToOperator(record);
								input.processElement(castRecord);
									MyMapFunc.map();
						}
					}
				}
		
			}
		}
	}
	
}















