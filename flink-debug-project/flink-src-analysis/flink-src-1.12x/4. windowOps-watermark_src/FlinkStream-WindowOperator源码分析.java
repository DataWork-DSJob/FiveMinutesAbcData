getCurrentKey:50, InternalKeyContextImpl (org.apache.flink.runtime.state.heap)
get:136, StateTable (org.apache.flink.runtime.state.heap)
getInternal:57, AbstractHeapAppendingState (org.apache.flink.runtime.state.heap)
get:84, HeapListState (org.apache.flink.runtime.state.heap)
get:43, HeapListState (org.apache.flink.runtime.state.heap)



// Processes one element that arrived on this input of the {@link MultipleInputStreamOperator}
/**
*	�������� 
*/

Input.processElement()

// stream-runtime-operators-window_src1.12.2 
WindowOperator.processElement(record){ // WindowOperator.processElement
    final Collection<W> elementWindows = windowAssigner.assignWindows();
    final K key = this.<K>getKeyedStateBackend().getCurrentKey(); // ȡǰ��keyBy��Ӧ��key;
    // MergingWindowAssigner ��ʲô?
    if (windowAssigner instanceof MergingWindowAssigner) {

    }else{ //����1��������������;
        for (W window: elementWindows) {
            if (isWindowLate(window)) { // drop if the window is already late
                continue;
            }
			windowState.setCurrentNamespace(window);
            windowState.add(element.getValue());{// HeapListState.add()
                map.get(namespace);{// StateTable.get()
					int keyGroupIndex= keyContext.getCurrentKeyGroupIndex();{// InternalKeyContextImpl.getCurrentKeyGroupIndex()
						return currentKeyGroupIndex;
						{
							// �ؼ�������� numberOfKeyGroups, һ����Ĭ�ϵ� ��󲢷� 128; 
							this.keyContext.setCurrentKeyGroupIndex(KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups));{//KeyGroupRangeAssignment.assignToKeyGroup
								return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);{
									return MathUtils.murmurHash(keyHash) % maxParallelism;
								}
							}
						}
					}
                    get(keyContext.getCurrentKey(), keyGroupIndex, namespace);
                        -> stateMap.get(key, namespace);
                            -> CopyOnWriteStateMap.get(key,namespace): ������ж� key:DeviceKey.equals()
                }
            }
            TriggerResult triggerResult = triggerContext.onElement(element);{ //WindowOperator$Context.onElement()
                return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);{
                    if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                    }else{ //
                        ctx.registerEventTimeTimer(window.maxTimestamp());
                            -> InternalTimerServiceImpl.registerEventTimeTimer(window, time);
                                -> eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
                        return TriggerResult.CONTINUE;
                    }
                }
            }
            if (triggerResult.isFire()) {
                ACC contents = windowState.get();
                emitWindowContents(window, contents);
            }
            if (triggerResult.isPurge()) {
                windowState.clear();
            }
            // ������ �ж�ʱ���Key�Ƿ����:  timestamp == timer.getTimestamp() && key.equals(timer.getKey()
            registerCleanupTimer(window);{//WindowOperator.registerCleanupTimer()
                if (windowAssigner.isEventTime()) triggerContext.registerEventTimeTimer(cleanupTime);
                    -> InternalTimerServiceImpl.registerEventTimeTimer(){
                        eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));{//HeapPriorityQueueSet.add()
                            //HashMap.putIfAbsent(key:TimerHeapInternalTimer,value: TimerHeapInternalTimer)
                            getDedupMapForElement(element).putIfAbsent(element, element) == null && super.add(element);{ //HashMap.putIfAbsent() -> HashMap.putVal()
                                if (p.hash == hash && ((k = p.key) == key || (key != null && key.equals(k)))){ // �������� key:TimerHeapInternalTimer ����.equal�Ƚ�;
                                    {// TimerHeapInternalTimer.equals()
                                        if (this == o)  return true;
                                        if (o instanceof InternalTimer) {
                                            return timestamp == timer.getTimestamp() // ��ʱ����Ƚ�;
                                                        && key.equals(timer.getKey()) // ��key:DeviceKey.equals()�Ƚ�;
                                                        && namespace.equals(timer.getNamespace()); //��namespace �Ƚ�
                                        }
                                    }
                                    e = p;
                                }
                            }
                        }
                    }
            }

        }
    }
}




















// Window().. GroupWindowAggregate(groupBy=...) �߳�
StreamTaskNetworkInput.emitNext(DataOutput<T> output){
    while (true) {
        if (result.isFullRecord()) {
            processElement(deserializationDelegate.getInstance(), output);{
                if (recordOrMark.isRecord()){ // ������,ֱ��������;
                    output.emitRecord(recordOrMark.asRecord());{//OneInputStreamTask$StreamTaskNetworkOutput.emitRecord()
                        operator.processElement(record);{
                            // ���ڴ�������:
                            WindowOperator.processElement(record){

                            }

                        }
                    }

                }else if (recordOrMark.isWatermark()) { // ��WM, ���ھۺϾ���Watermark;

                    statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);{ //StatusWatermarkValve.inputWatermark()

                        if (watermarkMillis > channelStatuses[channelIndex].watermark) {
                            channelStatuses[channelIndex].watermark = watermarkMillis;

                            findAndOutputNewMinWatermarkAcrossAlignedChannels();{
                                output.emitWatermark(new Watermark(lastOutputWatermark));
                                    -> operator.processWatermark(watermark);
                                        -> timeServiceManager.advanceWatermark(mark); -> service.advanceWatermark(watermark.getTimestamp());{// InternalTimerServiceImpl.advanceWatermark()
                                            while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
                                                eventTimeTimersQueue.poll();
                                                triggerTarget.onEventTime(timer);{//WindowOperator.onEventTime()

                                                    WindowOperator.onEventTime(){
                                                        if (triggerContext.onEventTime(timer.getTimestamp())) {
                                                            // ���ڵĴ��ھۺ����� ���ɽ���߼�; ?
                                                            emitWindowResult(triggerContext.window);{//AggregateWindowOperator.emitWindowResult()
                                                                windowFunction.prepareAggregateAccumulatorForEmit(window);
                                                                BaseRow aggResult = aggWindowAggregator.getValue(window);
                                                                BaseRow previousAggResult = previousState.value();
                                                                if (previousAggResult != null) {
                                                                    collector.collect(reuseOutput);
                                                                }
                                                            }
                                                        }

                                                    }
                                                }
                                            }
                                        }
                            }
                        }

                    }
                }else if (recordOrMark.isLatencyMarker()) {

                }

            }
        }
    }

}





# 1. Stream�� WindowOperator����Դ��


WindowOperator.onEventTime(){
    triggerContext.window = timer.getNamespace();
    TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

    if (triggerResult.isFire()) {
        ACC contents = windowState.get();
        if (contents != null) {
            emitWindowContents(triggerContext.window, contents);{
                userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);//InternalIterableWindowFunction.process()
                    -> wrappedFunction.apply(key, window, input, out);{// TestKeyedStateByPojo$3.apply()
                        // ��������û��Զ���� My WindowFunction.apply()
                        MyWindowFunction.apply(){
                            valueState.value();{// HeapValueState.value()
                                final V result = stateTable.get(currentNamespace);{
                                    return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);{
                                        StateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroupIndex);
                                        return stateMap.get(key, namespace); {//CopyOnWriteStateMap.get()
                                            // ���淽������ �Ѵ��ڵ�key:DeviceKey, ����� key.equals(eKey) �����ж�;
                                            // ��� keyBy().window() ����Ҫ��keyedState,Ҫ��ȷkey��Ҫ�Ը�Pojo: DeviceKey:  hashCode(),equals() ����д,������ȷ��ȡ;
                                        }
                                    }
                                }
                            }
                        }
                    }
            }
        }
    }

    if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
        clearAllState(triggerContext.window, windowState, mergingWindows);{//WindowOperator.clearAllState()
            windowState.clear();
                -> stateTable.remove(currentNamespace);//StateTable.remove
                    -> remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);{//StateTable.remove
                        stateMap.remove(key, namespace); 
                    }
        }
    }

}





// Flink SQL�� WindowOperator:? ��һ��? ���ں����߼�1: ��ÿ���¼�(Record)���ۼ� : ʱ�䴰��groupBy����,���Ǵ� WindowOperator����;
WindowOperator.processElement(record){
    if (windowAssigner.isEventTime()) {
        timestamp = inputRow.getLong(rowtimeIndex);
    }
    // �������Ӱ��Ĵ���
    Collection<W> affectedWindows = windowFunction.assignStateNamespace(inputRow, timestamp);
    for (W window : affectedWindows) {
        windowState.setCurrentNamespace(window);
        windowAggregator.setAccumulators(window, acc);
        if (BaseRowUtil.isAccumulateMsg(inputRow)) {
            // ���ĵĴ����ۼ��߼�;
            windowAggregator.accumulate(inputRow);{ //GroupingWindowAggsHandler$56
                // ���� StreamExecGroupWindowAggregateBase.createAggsHandler() ���ɵ�, ��������ܰ���sum,count,avg,last_value,MulitArgSum�ȶ������;
                // �������ۺϺ���, Ӧ����ÿ������,��������� .accumulate();
                windowAggregator = {GroupingWindowAggsHandler$56@10703}
                     agg0_sum = 1
                     agg0_sumIsNull = false
                     agg1_count1 = 1
                     agg1_count1IsNull = false
                     function_org$apache$flink$table$planner$functions$aggfunctions$LastValueAggFunction$IntLastValueAggF = {LastValueAggFunction$IntLastValueAggFunction@10752} "IntLastValueAggFunction"
                     function_com$bigdata$streaming$flink$mystudy$streamsql$sqlfunction$windowed$simpletest$MultiArgSumAg = {MultiArgSumAggFunc@10753} "MultiArgSumAggFunc"
                     converter = {DataFormatConverters$PojoConverter@10754}
                     converter = {DataFormatConverters$PojoConverter@10755}
                     agg2_acc_internal = {GenericRow@10756} "(+|1,-9223372036854775808)"
                     converter = {DataFormatConverters$PojoConverter@10759}

                MultiArgSumAg.accumulate(){

                }

                IntLastValueAggFunction.accumulate(){

                }
            }
        }else{
            windowAggregator.retract(inputRow);
        }
        acc = windowAggregator.getAccumulators();
        windowState.update(acc);
    }

}


// 2 �������Ӻ����߼�2: ���ڽ���,���;
WindowOperator.onEventTime(){
    if (triggerContext.onEventTime(timer.getTimestamp())) {
        // ���ڵĴ��ھۺ����� ���ɽ���߼�; ?
        emitWindowResult(triggerContext.window);{//AggregateWindowOperator.emitWindowResult()
            windowFunction.prepareAggregateAccumulatorForEmit(window);
            BaseRow aggResult = aggWindowAggregator.getValue(window);
            BaseRow previousAggResult = previousState.value();
            if (previousAggResult != null) {
                collector.collect(reuseOutput);
            }
        }
    }
}







