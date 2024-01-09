
BaseAlignedWindowAssigner
SlidingEventTimeWindows
MergingWindowAssigner
	DynamicEventTimeSessionWindows, DynamicProcessingTimeSessionWindows
	EventTimeSessionWindows, ProcessingTimeSessionWindows


Trigger
EventTime模式
	EventTimeTrigger: 一次触发,watermark大于窗口结束时间时触发; fires once the watermark passes the end of the window to which a pane belongs.
	ContinuousEventTimeTrigger,多次触发 ,基于event time的固定时间间隔;  continuously fires based on a given time interval. 
ProcessTime模式: 
	ProcessingTimeTrigger: 一次触发,machine time大于窗口结束时间时触发; fires once the current system time passes the end of the window to which a pane
	ContinuousProcessingTimeTrigger: 多次触发,基于processing time的固定时间间隔; continuously fires based on a given time interval as measured by the clock of the machine on which the job is running.
	ProcessingTimeoutTrigger: can turn any Trigger into a timeout Trigger
其他模式: 
	CountTrigger: 多次触发,基于element的固定条数; fires once the count of elements in a pane reaches the given count.
	DeltaTrigger: 多次触发,当前element与上次触发trigger的element做delta计算,超过threshold(阈值)时触发;  A Trigger that fires based on a DeltaFunction and a threshold.
	PurgingTrigger: trigger wrapper,当nested trigger时触发,额外会清理窗口当前的中间状态; can turn any Trigger into a purging Trigger. When the nested trigger fires, this will return a FIRE_AND_PURGE TriggerResult.
	NeverTrigger: never fires, as default Trigger for GlobalWindows.

Triggers接口调用情况
	onElement()	数据（element）被加入window的时候会调用该函数
	onEventTime() 	当一个注册的Event-Time定时器触发
	onProcessingTime() 	当一个注册的Processing-Time定时器触发
	onMerge()	与有状态触发器(stateful triggers)和当两个窗口整合的时候整合(merge)状态相关。例如使用session windows
	clear()	window清理数据需要

TriggerResult可以是下面几种选择:
	CONTINUE	什么都不做
	FIRE	触发计算
	PURGE	删除窗口中的所有数据
	FIRE_AND_PURG	触发计算后删除窗口中所有数据



Triggerable 接口, 定义定时触发输出的算子; 其实现类包括
api类 
	BatchGroupedReduceOperator
	KeyedProcessOperator
	LegacyKeyedProcessOperator
co类:
	CoBroadcastWithKeyedOperator
	IntervalJoinOperator
	KeyedCoProcessOperator
	LegacyKeyedCoProcessOperator
window类
	WindowOperator

api类 



// Processes one element that arrived on this input of the {@link MultipleInputStreamOperator}
/**
*	输入算子 
*/


// stream-runtime-operators-window_src1.12.2 : 
org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.processElement(record){ // WindowOperator.processElement
    // WindowAssigner 接口在f112中有9个实现类: Tumbling2个(TumbEvent, TumbProcess), 滑动2个(SlidProcess, SlidEvent), GlobalWindowA, , 
	// 4个可合并窗口 MergingWindowAssigner, 都是Session会话类 : DynamicEventTimeSessionWindows, DynamicProcessingTimeSessionWindows, EventTimeSessionWindows, ProcessingTimeSessionWindows
	final Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());{
		// 固定Event窗口, 根据eventTime 算出窗口start时间戳: TimeWindow.getWindowStartWithOffset(timestamp) 
		TumblingEventTimeWindows.assignWindows(T element, long timestamp, WindowAssignerContext context) {
			if (timestamp > Long.MIN_VALUE) {
				if (staggerOffset == null) {
					staggerOffset = windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
				}
				// Long.MIN_VALUE is currently assigned when no timestamp is present
				long start = TimeWindow.getWindowStartWithOffset(timestamp, (globalOffset + staggerOffset) % size, size);
				return Collections.singletonList(new TimeWindow(start, start + size));
			} else {
				throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call " + "'DataStream.assignTimestampsAndWatermarks(...)'?");
			}
		}
		// 固定-处理时间 窗口: 根据当前系统时间算出窗口start: context.getCurrentProcessingTime();
		TumblingProcessingTimeWindows.assignWindows(element, timestamp, context) {
			final long now = context.getCurrentProcessingTime();
			if (staggerOffset == null) {
				staggerOffset = windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
			}
			long start = TimeWindow.getWindowStartWithOffset(now, (globalOffset + staggerOffset) % size, size);
			return Collections.singletonList(new TimeWindow(start, start + size));
		}
		// 其他窗口类型: 滑动窗口, 会话窗口等 
		
	}
    
	// if element is handled by none of assigned elementWindows
    boolean isSkippedElement = true;
	
	final K key = this.<K>getKeyedStateBackend().getCurrentKey(); // 取前面keyBy对应的key;
    // MergingWindowAssigner 就是可合并窗口, 一般是Session类 ; 
    if (windowAssigner instanceof MergingWindowAssigner) {
		MergingWindowSet<W> mergingWindows = getMergingWindowSet();
		for (W window : elementWindows) {
			
		}
    }else{ //Sliding 和Tumbling 窗口, 都走这里; 
        for (W window: elementWindows) {
			// return (windowAssigner.isEventTime() && (cleanupTime(window) <= internalTimerService.currentWatermark()));
            boolean isLateWindow = isWindowLate(window);{// WindowOperator.isWindowLate(window )
				// win.maxTimestamp + allowedLateness <= currentWatermark, 返回true, 表示lateWindow过期窗口; 
				// 业务含义为 watermark水位 超过 该窗口的[结束时间+允许延迟, 即过期时间]过期时间, 标志该窗口过期: lateWindow; 
				// 线程接收到 Watermark消息后 inputWatermark() -> operator.processWatermark() -> timeServiceManager.advanceWatermark() 赋值 internalTimerService.currentWatermark;
				return (windowAssigner.isEventTime()
					&& (cleanupTime(window) <= internalTimerService.currentWatermark()));{
						WindowOperator.cleanupTime(window): long {
							if (windowAssigner.isEventTime()) {
								// 取窗口的结束时间maxTimestamp + 最大允许延迟allowedLateness 为窗口过去时间,清理时间, cleanupTime; 
								long cleanupTime = window.maxTimestamp() + allowedLateness;
								return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
							} else {
								return window.maxTimestamp();
							}
						}
					}
			}
			if (isLateWindow) { // drop if the window is already late
                continue;
            }
			// 进到这里说明窗口没过期, 要累加聚合窗口数据; 不skip; 
			isSkippedElement = false;
			windowState.setCurrentNamespace(window);
			// 将单条数据添加到 窗口缓存状态 windowState: InternalAppendingState
            windowState.add(element.getValue());{// HeapListState.add()
				// f112 Debug时, 这里windowState对象是 HeapListState
				HeapListState.add(V value) {
					stateTable.transform(currentNamespace, value, reduceTransformation);{//StateTable.
						K key = keyContext.getCurrentKey();
						int keyGroup = keyContext.getCurrentKeyGroupIndex();
						StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);
						stateMap.transform(key, namespace, value, transformation);{
							CopyOnWriteStateMap.transform() {
								final StateMapEntry<K, N, S> entry = putEntry(key, namespace);
								// 这里的 transformation.apply() 最后就是调 Reducer/Aggregate算子,进行reduce操纵; 
								entry.state = transformation.apply((entry.stateVersion < highestRequiredSnapshotVersion)
														? getStateSerializer().copy(entry.state)
														: entry.state, value);
								entry.stateVersion = stateMapVersion;
							}
						}
					}
				}
				// 其他版本, f19般的实现? 
                map.get(namespace);{// StateTable.get()
					int keyGroupIndex= keyContext.getCurrentKeyGroupIndex();{// InternalKeyContextImpl.getCurrentKeyGroupIndex()
						return currentKeyGroupIndex;
						{
							// 关键是这里得 numberOfKeyGroups, 一般是默认得 最大并发 128; 
							this.keyContext.setCurrentKeyGroupIndex(KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups));{//KeyGroupRangeAssignment.assignToKeyGroup
								return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);{
									return MathUtils.murmurHash(keyHash) % maxParallelism;
								}
							}
						}
					}
                    get(keyContext.getCurrentKey(), keyGroupIndex, namespace);
                        -> stateMap.get(key, namespace);
                            -> CopyOnWriteStateMap.get(key,namespace): 这里会判断 key:DeviceKey.equals()
                }
            }
			
			triggerContext.key = key;
			triggerContext.window = window;
			
			// 执行触发器 Trigger.onElement() 正常是 什么触发初始?
			// 单次元素触发, 执行 Trigger.onElement()方法计算触发类型: FIRE发射, CONTINUE继续等待不发射, PURGE清空数据; 
			// 默认是 EventTimeTrigger 或 ProcessingTimeTrigger 实现类; 还有 ContinuousEventTimeTrigger, ProcessingTimeoutTrigger, CountTrigger, DeltaTrigger等; 
			// 可以提取和自定义触发输出?
            TriggerResult triggerResult = triggerContext.onElement(element);{ //WindowOperator$Context.onElement()
                return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);{
					// Event模式默认是 EventTimeTrigger, 一次触发, watermark大于窗口结束时间时触发; 
					EventTimeTrigger.onElement(element, timestamp, window, TriggerContext ctx) {
						// 水位被推高了(才可能>当前窗口maxTs), 这当前窗口数据需要被输出; FIRE 发射; 
						if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
							// if the watermark is already past the window fire immediately
							return TriggerResult.FIRE;
						} else { // 窗口还在水位线内( 第一条/ 窗口内数据/ 水位更新异常?) 
							ctx.registerEventTimeTimer(window.maxTimestamp());{
								internalTimerService.registerEventTimeTimer(window, time);{//InterOperableNamingImpl.registerEventTimeTimer()
									eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
								}
							}
							return TriggerResult.CONTINUE;
						}
					}
					// Process模式, ProcessingTimeTrigger , 一次触发,machine time大于窗口结束时间时触发; 
					ProcessingTimeTrigger.onElement(element, timestamp, window, TriggerContext ctx) {
						ctx.registerProcessingTimeTimer(window.maxTimestamp());
						return TriggerResult.CONTINUE;
					}
                }
            }
			// FIRE发射数据, 则输出窗口数据 emitWindowContents(); 主要是调 userFunction.process()进行聚合计算; 
            if (triggerResult.isFire()) {
                ACC contents = windowState.get();
                emitWindowContents(window, contents);{
					timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
					userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
				}
            }
			// 如果触发结果为 PURGE删除状态, 则删除windowState.数据; 
            if (triggerResult.isPurge()) {
                windowState.clear();
            }
			// Registers a timer to cleanup the content of the window 
            // 核心是 判断时间和Key是否相等:  timestamp == timer.getTimestamp() && key.equals(timer.getKey()
            registerCleanupTimer(window);{//WindowOperator.registerCleanupTimer()
                // 用窗口结束时间+最大延迟得出过期时间 cleanupTime = window.maxTimestamp + allowedLateness;
				long cleanupTime = cleanupTime(window);
				if (windowAssigner.isEventTime()) {
					triggerContext.registerEventTimeTimer(cleanupTime);{
						internalTimerService.registerEventTimeTimer(window, time);{
							eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace)); {//HeapPriorityQueueSet.add()
								//HashMap.putIfAbsent(key:TimerHeapInternalTimer,value: TimerHeapInternalTimer)
								getDedupMapForElement(element).putIfAbsent(element, element) == null && super.add(element);{ //HashMap.putIfAbsent() -> HashMap.putVal()
									if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k)))){ // 在这里会对 key:TimerHeapInternalTimer 进行.equal比较;
										{// TimerHeapInternalTimer.equals()
											if (this == o)  return true;
											if (o instanceof InternalTimer) {
												return timestamp == timer.getTimestamp() // 对时间戳比较;
															&& key.equals(timer.getKey()) // 对key:DeviceKey.equals()比较;
															&& namespace.equals(timer.getNamespace()); //对namespace 比较
											}
										}
										e = p;
									}
								}
							}
						}
					}
				} else {
					triggerContext.registerProcessingTimeTimer(cleanupTime);{
						internalTimerService.registerProcessingTimeTimer(window, time);
					}
				}
            }

        }
    }
	
	// 前面已完成window窗口对该record数据的 windowState.add()缓存添加 或 FIRE发射给userFunction.process()聚合计算; 最后对延迟数据测流输出; 
	// side output input event if element not handled by any window
	// late arriving tag has been set windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
	if (isSkippedElement && isElementLate(element)) {
		if (lateDataOutputTag != null) {
			// 若配置了 .sideOutputLateData(lateData), 则进行测流输出; 
			sideOutput(element);{
				
			}
		} else {
			this.numLateRecordsDropped.inc();
		}
	}
}





// 线程接收到 Watermark消息后 inputWatermark() -> operator.processWatermark() -> timeServiceManager.advanceWatermark() 赋值 internalTimerService.currentWatermark;

org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.processElement(StreamElement recordOrMark, DataOutput<T> output) {
	if (recordOrMark.isRecord()) {
		output.emitRecord(recordOrMark.asRecord());
	} else if (recordOrMark.isWatermark()) {
		statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), flattenedChannelIndices.get(lastChannel), output);{//StatusWatermarkValve
			findAndOutputNewMinWatermarkAcrossAlignedChannels(output);{//StatusWatermarkValve.
				if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
					lastOutputWatermark = newMinWatermark;
					output.emitWatermark(new Watermark(lastOutputWatermark));{//OneInputStreamTask$StreamTaskNetworkOutput
						// 算子处理 Watermark对象/事件; 如 WindowOperator 算子调用父类 AbstractStreamOperator.processWatermark()方法 
						operator.processWatermark(watermark);{
							WindowOperator.processWatermark() {// 调父类 AbstractStreamOperator.processWatermark()
								if (timeServiceManager != null) {
									timeServiceManager.advanceWatermark(mark);{
										for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
											// 关于时间和水位的处理, 核心是 InternalTimerServiceImpl 类实现; 
											service.advanceWatermark(watermark.getTimestamp());{// InternalTimerServiceImpl.advanceWatermark()
											
												// 在这里, 把水位事件的时间(Watermark.timestamp)赋给算子的 internalTimerService.currentWatermark
												currentWatermark = time;
												while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
													eventTimeTimersQueue.poll();
													keyContext.setCurrentKey(timer.getKey());
													// triggerTarget : Triggerable, 即要触发的 聚合结构, 就是调 ProcessWindowFunction.process() 输出结果;  
													triggerTarget.onEventTime(timer); {
														//Triggerable 实现类包括:  BatchGroupedReduceOperator, KeyedProcessOperator, LegacyKeyedProcessOperator
														// CoBroadcastWithKeyedOperator, IntervalJoinOperator, WindowOperator
														// 最常见的 Window输出 
														WindowOperator.onEventTime(){
														
														}
														// 
														KeyedProcessOperator.onEventTime(){
															
														}
														
													}
												}
											}
										}
									}
								}
								output.emitWatermark(mark);
							}
						}
					}
				}
			}
		}
	}
}





/** WindowOperator.onEventTime() 
*	核心功能就是: 
		1. 计算是否输出:TriggerResult, 
		2. 调ProcessWindowFunction.process() 聚合输出; 
		3. 清理 windowState状态数据; 
*  这个 WindowOperator.onEventTime()是由水位事件触发的; 但Watermark 是哪个线程发出来的? 
*  
*/

// 核心就是 1. 计算是否输出:TriggerResult, 2. 调ProcessWindowFunction.process() 聚合输出; 3. 清理 windowState状态数据; 
WindowOperator.onEventTime(InternalTimer<K, W> timer){
	triggerContext.key = timer.getKey();
    triggerContext.window = timer.getNamespace();
    TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
	
	if (windowAssigner instanceof MergingWindowAssigner) {
		W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
		if (stateWindow == null) {
			return;
		} else {
			windowState.setCurrentNamespace(stateWindow);
		}
	} else { //正常的固定/滑动窗口走这里; 
		windowState.setCurrentNamespace(triggerContext.window);
		mergingWindows = null;
	}
	
	// 核心: 触发时机, 计算本窗口函数再 (EventTime模式?)下的 输出情况; FIRE, PURGE ?
	TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
	// 如果达到输出条件, 则输出; 其实就是调 ProcessWindowFunction.process() 聚合并输出; 
    if (triggerResult.isFire()) {
        ACC contents = windowState.get();
        if (contents != null) {
            emitWindowContents(triggerContext.window, contents);{//WindowOperator.emitWindowContents()
                timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
				// 调用用户定义的 ProcessFunc进行计算和输出; 
				userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);//InternalIterableWindowFunction.process()
                    -> wrappedFunction.apply(key, window, input, out);{// TestKeyedStateByPojo$3.apply()
						// 基于直接实现的 ProcessWindowFunction 方法; 
						MyRowProcessFunc.process() {
							
						}
						
						// 这里就是用户自定义的 My WindowFunction.apply()
                        MyWindowFunction.apply(){
                            valueState.value();{// HeapValueState.value()
                                final V result = stateTable.get(currentNamespace);{
                                    return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);{
                                        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);
                                        return stateMap.get(key, namespace); {//CopyOnWriteStateMap.get()
                                            // 里面方法对于 已存在的key:DeviceKey, 会调用 key.equals(eKey) 进行判断;
                                            // 如果 keyBy().window() 中需要用keyedState,要正确key需要对该Pojo: DeviceKey:  hashCode(),equals() 都重写,才能正确读取;
                                        }
                                    }
                                }
                            }
                        }
						

						
                    }
            }
        }
    }
	
	// 如果是PURGE删除状态, 则还状态删除; 
	if (triggerResult.isPurge()) {
		windowState.clear();
	}
	
	// 对于EventTime模式, 到清理时间了(窗口cleanupTime == 此定时器的时间) 清理所有状态数据; 
	boolean isCleanEvent = isCleanupTime(triggerContext.window, timer.getTimestamp());{// isCleanupTime(W window, long time)
		// 用窗口的结束+最大延迟得到 cleanupTime清理时间 , 于此定时器 Timer.timestamp 相当, 那就是这个清理; 
		return time == cleanupTime(window);{
			if (windowAssigner.isEventTime()) {
				long cleanupTime = window.maxTimestamp() + allowedLateness;
				return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
			} else {
				return window.maxTimestamp();
			}
		}
	}
    if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
        clearAllState(triggerContext.window, windowState, mergingWindows);{//WindowOperator.clearAllState()
            windowState.clear();{
				stateTable.remove(currentNamespace);//StateTable.remove
                    -> remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);{//StateTable.remove
                        stateMap.remove(key, namespace); 
                    }
			}
			triggerContext.clear();
			processContext.window = window;
			processContext.clear();
			
			mergingWindows.retireWindow(window);
            mergingWindows.persist();
        }
    }
	// 合并窗口? 
	if (mergingWindows != null) {
		// need to make sure to update the merging state in state
		mergingWindows.persist();
	}

}

