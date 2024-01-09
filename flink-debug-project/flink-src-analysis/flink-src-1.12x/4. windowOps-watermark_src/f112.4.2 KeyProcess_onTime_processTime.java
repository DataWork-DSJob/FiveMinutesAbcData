
/**
* 掉 ctx.timerService().registerProcessingTimeTimer(endTime); 
* 会触发自身 onTime()的定时调用 
* 
*/


InternalTimerServiceImpl.registerProcessingTimeTimer() {
	InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
	// 若添加成功, 
	boolean addSucceed = processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	if (addSucceed) {
		long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
		// check if we need to re-schedule our timer to earlier
		if (nextTimer != null) {
			nextTimer.cancel(false);
		}
		//  this::onProcessingTime 就是 InternalTimerServiceImpl.onProcessingTime() 
		ProcessingTimeCallback target = this::onProcessingTime
		nextTimer = processingTimeService.registerTimer(time, this::onProcessingTime); {// ProcessingTimeServiceImpl.registerTimer
			return timerService.registerTimer(timestamp,
				addQuiesceProcessingToCallback(processingTimeCallbackWrapper.apply(target))); {//SystemProcessingTimeService.registerTimer()
					// 定时执行该 任务 
					return timerService.schedule(wrapOnTimerCallback(callback, timestamp), delay, TimeUnit.MILLISECONDS);
			}
		}
	}
}
registerTimer:105, SystemProcessingTimeService (org.apache.flink.streaming.runtime.tasks)
registerTimer:68, ProcessingTimeServiceImpl (org.apache.flink.streaming.runtime.tasks)
registerProcessingTimeTimer:226, InternalTimerServiceImpl (org.apache.flink.streaming.api.operators)


getDedupMapForKeyGroup:147, HeapPriorityQueueSet (org.apache.flink.runtime.state.heap)
getDedupMapForElement:154, HeapPriorityQueueSet (org.apache.flink.runtime.state.heap)
add:120, HeapPriorityQueueSet (org.apache.flink.runtime.state.heap)
add:52, HeapPriorityQueueSet (org.apache.flink.runtime.state.heap)
registerProcessingTimeTimer:218, InternalTimerServiceImpl (org.apache.flink.streaming.api.operators)
registerProcessingTimeTimer:47, SimpleTimerService (org.apache.flink.streaming.api)
processElement:97, LogMergeFunction (com.eoi.jax.flink.job.process.logmerge)
processElement:40, LogMergeFunction (com.eoi.jax.flink.job.process.logmerge)
processElement:83, KeyedProcessOperator (org.apache.flink.streaming.api.operators)




StreamTask.invokeProcessingTimeCallback() {
	callback.onProcessingTime(timestamp);{//InternalTimerServiceImpl.onProcessingTime()
		nextTimer = null;
		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			processingTimeTimersQueue.poll();
			triggerTarget.onProcessingTime(timer); {//KeyedProcessOperator.onProcessingTime()
				collector.eraseTimestamp();
				// 执行用户定义的 KeyedProcessFunction.onTimer() 方法 
				invokeUserFunction(TimeDomain.PROCESSING_TIME, timer); {//KeyedProcessOperator.invokeUserFunction()
					onTimerContext.timer = timer;
					userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);{
						// 用户自定义代码 , 默认 KeyedProcessFunction.onTimer() 是空方法; 
						LogMergeFunction.onTimer()
						
					}
					onTimerContext.timer = null;
				}
			}
		}
	}
}

onTimer:147, LogMergeFunction (com.eoi.jax.flink.job.process.logmerge)
invokeUserFunction:91, KeyedProcessOperator (org.apache.flink.streaming.api.operators)
onProcessingTime:76, KeyedProcessOperator (org.apache.flink.streaming.api.operators)
onProcessingTime:284, InternalTimerServiceImpl (org.apache.flink.streaming.api.operators)
onProcessingTime:-1, 276151678 (org.apache.flink.streaming.api.operators.InternalTimerServiceImpl$$Lambda$563)
invokeProcessingTimeCallback:1327, StreamTask (org.apache.flink.streaming.runtime.tasks)

