

// flink_1.15_src

DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(Configuration configuration, JobType jobType) {
	JobManagerOptions.SchedulerType schedulerType = ClusterOptions.getSchedulerType(configuration);
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








