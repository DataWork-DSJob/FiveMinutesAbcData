


// IDEA 测试, MiniCluster.start()

/** flink-runtime_1.9.3: metrics
* 
*/
MiniCluster.start(){
	initializeIOFormatClasses(configuration);
	
	metricRegistry = createMetricRegistry(configuration);{
		config = MetricRegistryConfiguration.fromConfiguration(config);
		Collection<ReporterSetup> reporterConfigurations = ReporterSetup.fromConfiguration(config);
		return new MetricRegistryImpl( config, reporterConfigurations);{
			this.maximumFramesize = config.getQueryServiceMessageSizeLimit();
			this.reporters = new ArrayList<>(4);
			this.executor = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("Flink-MetricRegistry"));
			
			if (reporterConfigurations.isEmpty()) {
				
			} else { // 如果配置的 metrics 数量不为空时, 进入这里 初始化; 
				for (ReporterSetup reporterSetup : reporterConfigurations) {
					// 默认10 seconds 秒 报告一次; 
					TimeUnit timeunit = TimeUnit.SECONDS;
					long period = 10;
					String[] interval = configuredPeriod.get().split(" "); // metrics.reporter.kafka.interval 字段配置吧? 
					period = Long.parseLong(interval[0]);
					
					final MetricReporter reporterInstance = reporterSetup.getReporter();
					final String className = reporterInstance.getClass().getName();
					if (reporterInstance instanceof Scheduled) {
						
						// ! 启动定时发送report线程: 为每个Metric Reporter类启动1个监控线程, 触发执行 reporter.report();
						executor.scheduleWithFixedDelay(
							new MetricRegistryImpl.ReporterTask((Scheduled) reporterInstance), period, period, timeunit); {
								MetricRegistryImpl.ReporterTask.run(){
									reporter.report(); {
										
										// Prometheus 监控线程: 
										PrometheusPushGatewayReporter.report(){
											// 普米的JVM静态 收集器, 把 收集器采集到的数据, 发送到pushgateway上; 
											CollectorRegistry defaultRegistry = CollectorRegistry.defaultRegistry;
											pushGateway.push(defaultRegistry, jobName);
										}
										
										DatadogHttpReporter.report();
										InfluxdbReporter.report();
										Slf4jReporter.report();
										JMXReporter.report();// 只继承 MetricReporter, 没用实现 Scheduled接口;
										
										
										// 自己写的 KafkaReporter 
										KafkaReporter.report();
										
									}
								}
								
							}
					}
				}
				
			}
		}
	}
}



/** JobManager 进程中的 Metrics初始化
 *
 *
 */

new JobMaster(){
	this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);{//DefaultJobManagerJobMetricGroupFactory.
		return jobManagerMetricGroup.addJob(jobGraph);{
			JobID jobId = job.getJobID();
			String jobName = job.getName();
			currentJobGroup = jobs.get(jobId);
			if (currentJobGroup == null || currentJobGroup.isClosed()) {
				currentJobGroup = new JobManagerJobMetricGroup(registry, this, jobId, jobName);
				jobs.put(jobId, currentJobGroup);
			}
			return currentJobGroup;
		}
	}
	this.schedulerNG = createScheduler(jobManagerJobMetricGroup);{
		return schedulerNGFactory.createInstance();{
			
			DefaultSchedulerFactory.createInstance(){
				return new DefaultScheduler(){
					super();{// new SchedulerBase()
						
					}
				}
			}
			
		}
	}
		- return new LegacyScheduler();
			- ExecutionGraph newExecutionGraph = createExecutionGraph(currentJobManagerJobMetricGroup, shuffleMaster, partitionTracker);
				- return ExecutionGraphBuilder.buildGraph();{//ExecutionGraphBuilder.buildGraph()
					
					CheckpointStatsTracker checkpointStatsTracker = new CheckpointStatsTracker();{
						latestSnapshot = new CheckpointStatsSnapshot();
						registerMetrics(metricGroup);{
							metricGroup.gauge(NUMBER_OF_CHECKPOINTS_METRIC, new CheckpointsCounter());
							metricGroup.gauge(NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC, new InProgressCheckpointsCounter());
							metricGroup.gauge(NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC, new CompletedCheckpointsCounter());
							metricGroup.gauge(NUMBER_OF_FAILED_CHECKPOINTS_METRIC, new FailedCheckpointsCounter());
							metricGroup.gauge(LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC, new LatestRestoredCheckpointTimestampGauge());
							metricGroup.gauge(LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC, new LatestCompletedCheckpointSizeGauge());
							metricGroup.gauge(LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC, new LatestCompletedCheckpointDurationGauge());
							metricGroup.gauge(LATEST_COMPLETED_CHECKPOINT_ALIGNMENT_BUFFERED_METRIC, new LatestCompletedCheckpointAlignmentBufferedGauge());
							metricGroup.gauge(LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC, new LatestCompletedCheckpointExternalPathGauge());
						}
					}
					
					// create all the metrics for the Execution Graph
					metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
					metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
					metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));
					executionGraph.getFailoverStrategy().registerMetrics(metrics);
					return executionGraph;
					
				}
				
}

ResourceManager.onStart(){
	startResourceManagerServices();{
		initialize();
		leaderElectionService.start(this);
		
		registerSlotAndTaskExecutorMetrics();{
			jobManagerMetricGroup.gauge(MetricNames.TASK_SLOTS_AVAILABLE, () -> (long) slotManager.getNumberFreeSlots());
			jobManagerMetricGroup.gauge(MetricNames.TASK_SLOTS_TOTAL,() -> (long) slotManager.getNumberRegisteredSlots());
			
			jobManagerMetricGroup.gauge(MetricNames.NUM_REGISTERED_TASK_MANAGERS,() -> (long) taskExecutors.size());
			
		}
	}
}


	// 另外, JobManager初始化时, 也会注册
	 MetricUtils.instantiateJobManagerMetricGroup();
	


/** TaskManager进程种, 注册Metrics
* - 包括TM进程启动时, 注册JVM级别的监控
* - 包括TM 提交task时, 注册Task/Job级别的 metrics 
*/


// TM进程启动时 


// TM进程提交单个Task时 
TaskExecutor.submitTask(TaskDeploymentDescriptor,JobMasterId){
	
	jobInformation = tdd.getSerializedJobInformation()
                                .deserializeValue(getClass().getClassLoader());
	
	taskInformation = tdd.getSerializedTaskInformation()
                                .deserializeValue(getClass().getClassLoader());
	TaskMetricGroup taskMetricGroup = taskManagerMetricGroup.addTaskForJob();
	
	Task task = new Task();{
        this.inputGates = new IndexedInputGate[gates.length];
        int counter = 0;
        for (IndexedInputGate gate : gates) {
            inputGates[counter++] = new InputGateWithMetrics( gate, metrics.getIOMetricGroup().getNumBytesInCounter());
        }
		
		((NettyShuffleEnvironment) shuffleEnvironment).registerLegacyNetworkMetrics(
                            metrics.getIOMetricGroup(), resultPartitionWriters, gates);{
			NettyShuffleMetricFactory.registerLegacyNetworkMetrics(metricGroup, producedPartitions, inputGates);{
				// add metrics for buffers
				final MetricGroup buffersGroup = metricGroup.addGroup(METRIC_GROUP_BUFFERS_DEPRECATED);
				
				// similar to MetricUtils.instantiateNetworkMetrics() but inside this IOMetricGroup
				// (metricGroup)
				final MetricGroup networkGroup = metricGroup.addGroup(METRIC_GROUP_NETWORK_DEPRECATED);
				final MetricGroup outputGroup = networkGroup.addGroup(METRIC_GROUP_OUTPUT);
				final MetricGroup inputGroup = networkGroup.addGroup(METRIC_GROUP_INPUT);
			}
		}
							
	}
	taskMetricGroup.gauge(MetricNames.IS_BACKPRESSURED, task::isBackPressured);
}







AbstractMetricGroup.addMetric(String name, Metric metric) 
	Metric prior = metrics.put(name, metric);
	registry.register(metric, name, this); {//MetricRegistryImpl.register()
		if (reporters != null) {
			for (int i = 0; i < reporters.size(); i++) {
				MetricReporter reporter = reporters.get(i);
				FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
				reporter.notifyOfAddedMetric(metric, metricName, front); {
					// prom 
					PrometheusReporter.notifyOfAddedMetric();
					
					// 
					InfluxdbReporter
					Slf4jReporter
					
					// 自己写的KafkaReport
					
				
				}
			}
		}
		
		queryService.addMetric(metricName, metric, group);
		
		viewUpdater.notifyOfAddedView((View) metric);
	}
}




{
	MetricGroup jvm = metricGroup.addGroup("JVM");
	// 有 AbstractMetricGroup 有counter() , gauge(), histogram() 等4个方法, 底层都是调 addMetric()
	metrics.gauge("ClassesLoaded", mxBean::getTotalLoadedClassCount); {// AbstractMetricGroup.gauge()
		// 最后都是走的 addMetric()
		addMetric(name, gauge);{//AbstractMetricGroup.addMetric()
			Metric prior = metrics.put(name, metric);
			registry.register(metric, name, this); {//MetricRegistryImpl.register()
				if (reporters != null) {
					for (int i = 0; i < reporters.size(); i++) {
						MetricReporter reporter = reporters.get(i);
						FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
						reporter.notifyOfAddedMetric(metric, metricName, front); {
							// prom 
							PrometheusReporter.notifyOfAddedMetric();
							
							// 
							InfluxdbReporter
							Slf4jReporter
							
							// 自己写的KafkaReport
							
						
						}
					}
				}
				
				queryService.addMetric(metricName, metric, group);
				
				viewUpdater.notifyOfAddedView((View) metric);
			}
		}
		
	}
	
}

	{ // 很多地方添加Metric
	//TaskManagerRunner.startTaskManager() 时,  这里加载 JVM的5个 指标组 Group; 
	MetricUtils.instantiateStatusMetrics();{
		MetricGroup jvm = metricGroup.addGroup("JVM");
		
		instantiateClassLoaderMetrics(jvm.addGroup("ClassLoader"));
		instantiateGarbageCollectorMetrics(jvm.addGroup("GarbageCollector"));
		instantiateMemoryMetrics(jvm.addGroup("Memory"));
		instantiateThreadMetrics(jvm.addGroup("Threads"));
		instantiateCPUMetrics(jvm.addGroup("CPU"));
	}

	// TaskManagerRunner.startTaskManager() 时, 注册Shuffle 和 Network的 MetricGroup; 
	NettyShuffleMetricFactory.registerShuffleMetrics(){
		registerShuffleMetrics(METRIC_GROUP_NETWORK_DEPRECATED, metricGroup, networkBufferPool); {
			MetricGroup networkGroup = metricGroup.addGroup(groupName);
			networkGroup.<Integer, Gauge<Integer>>gauge(METRIC_TOTAL_MEMORY_SEGMENT,networkBufferPool::getTotalNumberOfMemorySegments);
			
			networkGroup.<Integer, Gauge<Integer>>gauge(METRIC_AVAILABLE_MEMORY_SEGMENT,networkBufferPool::getNumberOfAvailableMemorySegments);
		}
		registerShuffleMetrics(METRIC_GROUP_NETTY, metricGroup.addGroup(METRIC_GROUP_SHUFFLE), networkBufferPool);
	}
	
	// 另外, JobManager初始化时, 也会注册
	 MetricUtils.instantiateJobManagerMetricGroup();
	
	// JM ? CK启动时,会注册 12个指标?
	CheckpointStatsTracker.registerMetrics();
	
	// 启动RM服务时, 会注册3个Group; 
	ResourceManager.registerSlotAndTaskExecutorMetrics();
	
	// Task添加1个Task时, addTaskForJob() -> addTask()
	 new TaskIOMetricGroup(this);
	}







/** Prometheus 4中Collector.collect() 采集指标具体实现; 
 *	分成 TaskManager 和 JobManager 进程 中的指标; 
 * TaskManger 
 * JobManager 
 * 	
 */

{// TaskManager 中的 Metric: Gauge 
	
	//对于KafkaConsumer_fetch_rate 指标, gauge 实现类: KafkaMetricWrapper: 
	flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper.getValue();{
		return this.kafkaMetric.value();{ // org.apache.kafka.common.metrics.KafkaMetric.value();
			long milliseconds = time.milliseconds();{
				return System.currentTimeMillis();
			}
			return measurableValue(milliseconds);{
				if (this.metricValueProvider instanceof Measurable) {
					return ((Measurable) metricValueProvider).measure(config, timeMs);
				}
			}
		}
	}
	
	// flink_taskmanager_job_task_Shuffle_Netty_Output_Buffers_outPoolUsage
	org.apache.flink.runtime.io.network.metrics.OutputBufferPoolUsageGauge.getValue();{
		for (ResultPartition resultPartition : resultPartitions) {
			BufferPool bufferPool = resultPartition.getBufferPool();
			usedBuffers += bufferPool.bestEffortGetNumOfUsedBuffers();
			bufferPoolSize += bufferPool.getNumBuffers();
		}
		return ((float) usedBuffers) / bufferPoolSize;
	}
	
	MetricUtils.$AttributeGauge.getValue();{
		return (T) server.getAttribute(objectName, attributeName);
	}
	
}



{ // JobManager 中的Gauge
	
	CompletedCheckpointsCounter.getValue(){
		return counts.getNumberOfCompletedCheckpoints();
	}
	
}


// 定时上报 reporter线程, 默认每个10秒上报一次;

MetricRegistryImpl.ReporterTask() {
	reporter.report();
}

















