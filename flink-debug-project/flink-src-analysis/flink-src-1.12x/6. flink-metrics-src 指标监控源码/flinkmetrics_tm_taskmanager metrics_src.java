


// IDEA 测试, MiniCluster.start()


/** TaskManager进程种, 注册Metrics
* - 包括TM进程启动时, 注册JVM级别的监控
* - 包括TM 提交task时, 注册Task/Job级别的 metrics 
*/


// TM进程启动和初始化时: new TaskManagerRunner()




// flink-1.12.x_src: runtime 
// 可知 作为tm_id的 taskManagerId 就是 resourceId字符串; 其来自 taskmanager.resource-id 或 rpcAddress:rpcPort 拼接; 
TaskManagerRunner.main(){
	runTaskManagerProcessSecurely(args);{
		Configuration configuration = loadConfiguration(args);
		runTaskManagerProcessSecurely(checkNotNull(configuration));{//TaskManagerRunner.
			final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
			FileSystem.initialize(configuration, pluginManager);
			SecurityUtils.getInstalledContext().runSecured(() -> runTaskManager(configuration, pluginManager)); {
				TaskManagerRunner.runTaskManager(configuration, pluginManager){
					// 最总要的TM初始化 
					TaskManagerRunner taskManagerRunner = new TaskManagerRunner();{
						rpcService = createRpcService(configuration, highAvailabilityServices);
						// 这里定义资源ID, 从
						this.resourceId = getTaskManagerResourceID( configuration, rpcService.getAddress(), rpcService.getPort());{
							// 1.先使用 taskmanager.resource-id配置有(yarn是containerId, k8s是?) 配置值;
							// 2. 再使用 rpcAddress:rpcPort-new AbstractID();
							// 3. 如果rpcAddress为空, 则使用 InetAddress.getLocalHost().getHostName() 本机IP;
							// 最后 TM监控 new TaskManagerMetricGroup(hostname,taskManagerId) 时, 就是用的 resourceId; 
							String resourceId = config.getString(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID,
											StringUtils.isNullOrWhitespaceOnly(rpcAddress)
													? InetAddress.getLocalHost().getHostName()+ "-" + new AbstractID().toString().substring(0, 6)
													: rpcAddress+ ":"+ rpcPort+ "-" + new AbstractID().toString().substring(0, 6));
							String metadata = config.getString(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, "");
							return new ResourceID(resourceId, metadata);
							
						}
						
						metricRegistry = new MetricRegistryImpl();
						metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);
						taskExecutorService =taskExecutorServiceFactory.createTaskExecutor();
						
					}
					 return taskManagerRunner.getTerminationFuture().get().getExitCode();
				}
			}
		}
	}
}




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




// 定时上报 reporter线程, 默认每个10秒上报一次;

MetricRegistryImpl.ReporterTask() {
	reporter.report();
}

















