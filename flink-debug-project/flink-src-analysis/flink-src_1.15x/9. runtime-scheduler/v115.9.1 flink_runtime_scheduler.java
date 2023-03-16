

// flink_1.15_src

Job的资源管理 scheduler生成时机: 
- Client端发出submitJob请求并提交作业后, JM进程接受消息并执行 submitJob()方法; 

// JM进程, Akka消息路由Dispatcher
Dispatcher.submitJob() -> Dispatcher.internalSubmitJob()
	waitForTerminatingJob() -> Dispatcher.persistAndRunJob()
		Dispatcher.createJobMasterRunner() 
			DefaultSlotPoolServiceSchedulerFactory.fromConfiguration();





DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(Configuration configuration, JobType jobType) {
	JobManagerOptions.SchedulerType schedulerType = ClusterOptions.getSchedulerType(configuration);{
        // jobmanager.scheduler= Adaptive 时; (有3个值: Ng, Adaptive, AdaptiveBatch, 默认Ng);
		boolean isAdaptiveSchedulerEnabled = isAdaptiveSchedulerEnabled(configuration);{
			if (configuration.contains(JobManagerOptions.SCHEDULER)) {// 包含 jobmanager.scheduler 关键Key吗?
				return configuration.get(JobManagerOptions.SCHEDULER) == JobManagerOptions.SchedulerType.Adaptive;
			} else {
				return System.getProperties().containsKey("flink.tests.enable-adaptive-scheduler");
			}
		}
		// scheduler-mode=reactive 时; 也返回 Adaptive; 
		boolean isReactiveMode = isReactiveModeEnabled(configuration);{
			return configuration.get(JobManagerOptions.SCHEDULER_MODE) == SchedulerExecutionMode.REACTIVE;
		}
		
		if (isAdaptiveSchedulerEnabled || isReactiveMode) {
            return JobManagerOptions.SchedulerType.Adaptive;
        } else {
            return configuration.get(JobManagerOptions.SCHEDULER);
        }
	}
	if (schedulerType == JobManagerOptions.SchedulerType.Adaptive && jobType == JobType.BATCH) {
		schedulerType = JobManagerOptions.SchedulerType.Ng;
	}
	switch (schedulerType) {
		case Ng:
			schedulerNGFactory = new DefaultSchedulerFactory();
			slotPoolServiceFactory = new DeclarativeSlotPoolBridgeServiceFactory(
							SystemClock.getInstance(),
							rpcTimeout,
							slotIdleTimeout,
							batchSlotTimeout,
							getRequestSlotMatchingStrategy(configuration, jobType));
			break;
		case Adaptive:
			schedulerNGFactory = getAdaptiveSchedulerFactoryFromConfiguration(configuration);{//DefaultSlotPoolServiceSchedulerFactory.getAdaptiveSchedulerFactoryFromConfiguration()
				// jobmanager.adaptive-scheduler.resource-wait-timeout, 默认值是 -1, 会一直等待直到拥有足够资源。如果你想要在没有拿到足够的 TaskManager 的一段时间后关闭，可以配置这个参数。
				Duration allocationTimeoutDefault = JobManagerOptions.RESOURCE_WAIT_TIMEOUT.defaultValue();
				// jobmanager.adaptive-scheduler.resource-stabilization-timeout 默认值是 0, 有足够的资源就会启动 Job, 可能会造成Job重启一次; 。 希望等待资源稳定后再启动 Job，那么可以增加这个配置的值。
				Duration stabilizationTimeoutDefault =JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT.defaultValue();
				if (configuration.get(JobManagerOptions.SCHEDULER_MODE) == SchedulerExecutionMode.REACTIVE) {
					allocationTimeoutDefault = Duration.ofMillis(-1);
					stabilizationTimeoutDefault = Duration.ZERO;
				}
				final Duration initialResourceAllocationTimeout =configuration.getOptional(JobManagerOptions.RESOURCE_WAIT_TIMEOUT).orElse(allocationTimeoutDefault);
				final Duration resourceStabilizationTimeout =configuration.getOptional(JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT).orElse(stabilizationTimeoutDefault);
				return new AdaptiveSchedulerFactory(initialResourceAllocationTimeout, resourceStabilizationTimeout);
			}
			slotPoolServiceFactory = new DeclarativeSlotPoolServiceFactory(SystemClock.getInstance(), slotIdleTimeout, rpcTimeout);
			break;
		case AdaptiveBatch:
			schedulerNGFactory = new AdaptiveBatchSchedulerFactory();
			slotPoolServiceFactory = new DeclarativeSlotPoolBridgeServiceFactory(
							SystemClock.getInstance(),
							rpcTimeout,
							slotIdleTimeout,
							batchSlotTimeout,
							getRequestSlotMatchingStrategy(configuration, jobType));
			break;
		default:
			throw new IllegalArgumentException(String.format("Illegal value [%s] for config option [%s]", schedulerType, JobManagerOptions.SCHEDULER.key()));
	}
	return new DefaultSlotPoolServiceSchedulerFactory(slotPoolServiceFactory, schedulerNGFactory);
}



DefaultSlotPoolServiceSchedulerFactory.createScheduler() {
	return schedulerNGFactory.createInstance();{
		// ng 
		DeclarativeSlotPoolBridgeServiceFactory.createInstance();
		// case Adaptive: Reactive Mode, Adaptive  Mode, 
		AdaptiveSchedulerFactory.createInstance();
		// case AdaptiveBatch:
		AdaptiveBatchSchedulerFactory.createInstance();{}
	}
}



// 在JobMaster服务创建时, 实例化 Scheduler的对象: DefaultScheduler, AdaptiveScheduler, AdaptiveBatchScheduler

new JobMaster(new DefaultExecutionDeploymentTracker());{//new JobMaster() 构造函数中
		resourceManagerLeaderRetriever =highAvailabilityServices.getResourceManagerLeaderRetriever();
		this.schedulerNG = createScheduler(executionDeploymentTracker, jobManagerJobMetricGroup);{//DefaultSlotPoolServiceSchedulerFactory.createScheduler()
			return schedulerNGFactory.createInstance();{//DefaultSchedulerFactory.
				
				// 自适应调度NGFactory: case Adaptive: Reactive Mode, Adaptive  Mode, 
				AdaptiveSchedulerFactory.createInstance();{
					SlotSharingSlotAllocator slotAllocator = createSlotSharingSlotAllocator(declarativeSlotPool);
					return new AdaptiveScheduler();
				}
				// 默认的 NG调度, // ng 
				DefaultSchedulerFactory.createInstance();{
					DefaultSchedulerComponents schedulerComponents =createSchedulerComponents();
					restartBackoffTimeStrategy =RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory().create();
					return new DefaultScheduler();{super(){// SchedulerBase构造方法, 构建执行计划Graph
						this.executionGraph =createAndRestoreExecutionGraph();{
							ExecutionGraph newExecutionGraph =createExecutionGraph();
							inputsLocationsRetriever =new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);
							this.coordinatorMap = createCoordinatorMap();
						}}
					}
				}
				
				//  case AdaptiveBatch:
				AdaptiveBatchSchedulerFactory.createInstance();{}
				
		}
	}
}









