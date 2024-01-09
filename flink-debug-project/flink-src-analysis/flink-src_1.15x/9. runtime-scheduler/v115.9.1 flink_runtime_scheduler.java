
flik 1.15

调度有几个重要的组件：
    调度器：SchedulerNG及其子类、实现类
    调度策略：SchedulingStrategy及其实现类
    调度模式：ScheduleMode包含流和批的调度，有各自不同的调度模式
调度器是Flink作业执行的核心组件，管理作业执行的所有相关过程，包括JobGraph到ExecutionGraph的转换、作业生命周期管理（作业的发布、取消、停止）、作业的Task生命周期管理（Task的发布、取消、停止）、资源申请与释放、作业和Task的Failover等
	JobGraph到ExecutionGraph的转换
	作业生命周期管理（作业的发布、取消、停止）
	Task生命周期管理
	资源申请与释放
	作业和Task的Failover等
	

SchedulerNG, 内部的调度引擎，负责job的调度处理; 
	// ScheduleNG实际负责job调度处理，包括生成ExecutionGraph，作业的调度执行，任务出错处理等
	SchedulerBase
		DefaultScheduler
	AdaptiveScheduler

SchedulerNG 相关的接口: 
	void startScheduling();
	void cancel();
	boolean updateTaskExecutionState();
	ExecutionGraphInfo requestJob();
	
	deliverOperatorEventToCoordinator()
	deliverCoordinationRequestToCoordinator();
	


SchedulingStrategy : 调度分配策略, 分配 
	Component which encapsulates the scheduling logic. It can react to execution state changes and partition consumable events. Moreover, it is responsible for resolving task failures
	调度器是Flink作业执行的核心组件，管理作业执行的所有相关过程，包括JobGraph到ExecutionGraph的转换、作业生命周期管理（作业的发布、取消、停止）、作业的Task生命周期管理（Task的发布、取消、停止）、资源申请与释放、作业和Task的Failover等

interface  SchedulingStrategy {
	void startScheduling(); // 调度入口，触发调度器的调度行为
	
	void restartTasks(Set<ExecutionVertexID> verticesToRestart); //重启执行失败的Task，一般是Task执行异常导致
	
	// 当Execution改变状态时调用
	void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionStateexecutionState);
	
	// 当IntermediateResultPartition中的数据可以消费时调用
	void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId);
	
}

flink1.12 SchedulingStrategy, 调度策略有三种实现：
    EagerSchedulingStrategy：适用于流计算，同时调度所有的task
    LazyFromSourcesSchedulingStrategy：适用于批处理，当输入数据准备好时（上游处理完）进行vertices调度。
    PipelinedRegionSchedulingStrategy：以流水线的局部为粒度进行调度
	 *  PipelinedRegionSchedulingStrategy 是1.11加入的，从1.12 开始，将以 pipelined region 为单位进行调度。
	 *  pipelined region 是一组流水线连接的任务。这意味着，对于包含多个 region 的流程作业，在开始部署任务之前，它不再等待所有任务获取 slot。取而代之的是，一旦任何 region 获得了足够的任务 slot 就可以部署它。
	 *  对于批处理作业，将不会为任务分配 slot，也不会单独部署任务。取而代之的是，一旦某个 region 获得了足够的 slot，则该任务将与所有其他任务一起部署在同一区域中。

flink_1.15 SchedulingStrategy 有 种: 
	PipelinedRegionSchedulingStrategy：以流水线的局部为粒度进行调度
	VertexwiseSchedulingStrategy, 
		 * which schedules tasks in granularity of vertex (which indicates this strategy only supports ALL_EDGES_BLOCKING batch jobs). Note that this strategy implements SchedulingTopologyListener, so it can handle the updates of scheduling topology.



JobGraph和StreamGraph最重要的区别就是：对满足chainable条件的算子进行了chain
StreamGraph 经过优化后生成了 JobGraph，提交给 JobManager 的数据结构。
主要的变化有：
    StreamNode变成了JobVertex
    StreamEdge变成了JobEdge
    增加了IntermediateDataSet
    Forward相连的StreamNode合并(chain)成一个JobVertex



JobGraph : 进过operator chain 优化后的 作业执行拓扑图, 特点: 
 - 是 StreamGraph 经过优化后生成了 JobGraph，提交给 JobManager 的数据结构, 主要优化就是 对满足chainable条件的算子进行了chain; 
 - 主要的变化有：StreamNode变成了JobVertex; StreamEdge变成了JobEdge; 增加了IntermediateDataSet; Forward相连的StreamNode合并(chain)成一个JobVertex

	JobID
	JobVertex, 表示优化后chain在一起的一个逻辑Task, 包括 List<OperatorIDPair> operatorIDs
		class JobVertex {
			JobVertexID id; // 唯一ID , 如 92caaefe3a3c3f949152194a33baf0c4
			List<OperatorIDPair> operatorIDs;// 该Vertex顶点包含的所有算子, The IDs of all operators contained in this vertex.
			ArrayList<IntermediateDataSet> results; // 输出: List of produced data sets, one per writer.
			ArrayList<JobEdge> inputs;// 输入, List of edges with incoming data. One per Reader
		}
		
	JobEdge, 连接连个JobVertex的边; 
		class JobEdge {
			JobVertex target; //要连接到(下游)哪个Vertex顶点; The vertex connected to this edge.
			IntermediateDataSet source;// 来源, The data set at the source of the edge, may be null if the edge is not yet connected
			String shipStrategyName;// 是forwar直发,还是hash或者rebalance;  Optional name for the data shipping strategy (forward, partition hash, rebalance, ...), to be displayed in the JSON pla
		}
	LogicalVertex, 
	LogicalTopology, 
	JobCheckpointingSettings,

ExecutionGraph: 初始化过程中就会开始构建我们的ExecutionGraph，ExecutionGraph中有几个重要元素
    ExecutionJobVertex： 代表jobGraph中的一个JobVertex，是所有并行Task的集合
		class ExecutionJobVertex {
			JobVertex jobVertex;// 1个 JobVertex, 1跟 ExecutionJobVertex
			ExecutionVertex[] taskVertices; // 1个JobVertext 由多个并发,就多个 ExecutionVertex执行顶点 
			List<IntermediateResult> inputs; 
		}
	ExecutionVertex： 代表ExecutionJobVertex中并行task中的一个，一个ExecutionJobVertex可能同时有很多并行运行的ExecutionVertex
		class ExecutionVertex {
			ExecutionJobVertex jobVertex;//
			int subTaskIndex; 				// 子任务下标(0,1,2,,); 所以一个SubTask(并发)一个 ExecutionVertex
			ArrayList<InputSplit> inputSplits;
		}
	
	Execution： 代表ExecutionVertex的一次部署/执行，一个ExecutionVertex可能会有很多次Execution
	* 一个Job一个ExecutionJobVertex;  一个 SubTask对应一个ExecutionVertex, ExecutionVertex代表并行task种的一个; 


List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
executionGraph.attachJobGraph(sortedTopology){
    1. executionGraph第一步拿到了jobGraph中的有序JobVertex列表
    2. 接着一对一创建ExecutionJobVertex
    3. 根据producer并行度生成producedDataSets(IntermediateDataSet)
    4. 再根据自身并行度生成所属的ExecutionVertex[]
    5. 构建stateBackend信息和checkpointStorage信息等
    6. 最后完成executionGraph的拓扑构建executionTopology
}

我们看看executionGraph的核心拓扑结构ExecutionTopology是如何构建的
	第一步 先根据executionTopology构建rawPipelinedRegions，多个vertex能否组合成一个pipeline region的关键在于这个vertex的consumedResult.getResultType().isReconnectable()，如果支持重连，那么两个vertex之间就会进行拆分，划到不同的region。这里的isReconnectable就和我们的ResultPartitionType类型有关，流处理中的PIPELINED和PIPELINED_BOUNDED都是默认的false，在这种情况下所有的vertex其实都会放入同一个region。故我们日常的flink作业其实都只会生成一个pipeline region。
	第二步 根据不同的pipeline region构建自己的resultPartition信息，这个是为了构建后续的PartitionReleaseStrategy，决定一个resultPartition何时finish以及被release
	第三步 对vertex的coLocation情况进行校验，保证co-located tasks必须在同一个pipeline Region里。这里是因为后续的scheduling strategy里会保证不同pipeline region的调度部署是阶段隔离的，可能无法满足colocation-constraint


ExecutionState, 任务调度的过程 和状态 
    CREATED, 	最初任务处在Created状态
    SCHEDULED,	认为这个任务可以开始被调的时候，它会转到Scheduled状态，并开始申请资源
    DEPLOYING,  申请到Slot之后，它就转到Deploying状态来生成Task的描述
    INITIALIZING 发动到Worker上, 启动初始化阶段; 
    RUNNING, 	部署到worker节点后Task就启动起来, 成功启动后就是RUNNING 描述
    FINISHED,	有限流作业 (批作业)，所有数据处理完了，任务还会转到finished状态，标志任务完成。
    CANCELING,  其它受到影响的任务可能会被Cancel掉, 进入取消CANCELING状态; 
    CANCELED,	其它受到影响的任务可能会被Cancel掉并走到Canceled状态。
    FAILED,		有异常发生时，任务也会转到Failed的状态;
    RECONCILING,	

	
  CREATED  -> SCHEDULED -> DEPLOYING -> INITIALIZING -> RUNNING -> FINISHED
     |            |            |          |              |
     |            |            |    +-----+--------------+
     |            |            V    V
     |            |         CANCELLING -----+----> CANCELED
     |            |                         |
     |            +-------------------------+
     |
     |                                   ... -> FAILED
     V
 RECONCILING  -> INITIALIZING | RUNNING | FINISHED | CANCELED | FAILED

任务具有很多种不同状态，最初任务处在Created状态。当调度策略认为这个任务可以开始被调的时候，它会转到Scheduled状态，并开始申请资源，即Slot。申请到Slot之后，它就转到Deploying状态来生成Task的描述，并部署到worker节点上，再之后Task就会在worker节点上启动起来。成功启动后，它会在worker节点上转到running状态并通知JobMaster，然后在JobMaster端把任务的状态转到running。
；对于有限流的作业，一旦所有数据处理完了，任务还会转到finished状态，标志任务完成。当有异常发生时，任务也会转到Failed的状态，同时其它受到影响的任务可能会被Cancel掉并走到Canceled状态。


FailoverStrategy




// flink_1.15_src

Job的资源管理 scheduler生成时机: 
- Client端发出submitJob请求并提交作业后, JM进程接受消息并执行 submitJob()方法; 

// JM进程, Akka消息路由Dispatcher
Dispatcher.submitJob() -> Dispatcher.internalSubmitJob()
	waitForTerminatingJob() -> Dispatcher.persistAndRunJob()
		Dispatcher.createJobMasterRunner() 
			DefaultSlotPoolServiceSchedulerFactory.fromConfiguration();


// 1. Config and Init 

// 1.1 根据配置生成不同 SchedulerFactory 调度器的源码 ; 

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

// 1.2 不同调度器Factory 创建相应 SchedulerNg的 instance 

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




// 2. 在JobManager种, 收到作业job请求, 完成 submitJob() -> new JobMaster() 后, 由JobMasger启动作业; 
JobMaster.onStart() -> startJobExecution() -> startScheduling(); {
	// 从f113开始SchedulerNG 2个实现类: 
	schedulerNG.startScheduling();{
		// 默认 DefaultScheduler
		DefaultScheduler.startScheduling();{}
		
		// 配置 jobmanager.scheduler: adaptive, 自适应调度器 AdaptiveScheduler
		AdaptiveScheduler.startScheduling();{}
	}
}



// 2. 1 flink115 默认调度策略 DefaultScheduler, 非自适应策略,  其执行调度流程源码; 

maybeScheduleRegion:234, PipelinedRegionSchedulingStrategy (org.apache.flink.runtime.scheduler.strategy)
maybeScheduleRegions:216, PipelinedRegionSchedulingStrategy (org.apache.flink.runtime.scheduler.strategy)
startScheduling:150, PipelinedRegionSchedulingStrategy (org.apache.flink.runtime.scheduler.strategy)
startSchedulingInternal:262, DefaultScheduler (org.apache.flink.runtime.scheduler)
startScheduling:625, SchedulerBase (org.apache.flink.runtime.scheduler)
startScheduling:1010, JobMaster (org.apache.flink.runtime.jobmaster)


// f115_runtime_scheduler 
class DefaultScheduler {
	
	// 默认 DefaultScheduler
	DefaultScheduler.startScheduling();{// 调用SchedulerBase.startScheduling()
		mainThreadExecutor.assertRunningInMainThread();
		registerJobMetrics();
		operatorCoordinatorHandler.startAllOperatorCoordinators();
		startSchedulingInternal(); {// DefaultScheduler.startSchedulingInternal()
			transitionToRunning(); // Job FlinkStreamExample (xxx jobId) switched from state CREATED to RUNNING
			schedulingStrategy.startScheduling();{// PipelinedRegionSchedulingStrategy.startScheduling()
				
				// 一个Job一个ExecutionJobVertex;  一个 SubTask对应一个ExecutionVertex, ExecutionVertex (并发执行顶点)代表并行task种的一个; 
				// SchedulingPipelinedRegion对象中的 executionVertices: Map<ExecutionVertexID, DefaultExecutionVertex> 包含本次要执行的所有SubTask的对应的ExecutionVertex; 
				Set<SchedulingPipelinedRegion> sourceRegions = IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())
						.filter(this::isSourceRegion).collect(Collectors.toSet());
				maybeScheduleRegions(sourceRegions); {// PipelinedRegionSchedulingStrategy.maybeScheduleRegions
					List<SchedulingPipelinedRegion> regionsSorted = SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder( schedulingTopology, regions);
					Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
					// 如果由多个 SchedulingPipelinedRegion, 遍历一个个执行; 
					for (SchedulingPipelinedRegion region : regionsSorted) {
						maybeScheduleRegion(region, consumableStatusCache);{// PipelinedRegionSchedulingStrategy.maybeScheduleRegion(region)
							if (!areRegionInputsAllConsumable(region, consumableStatusCache)) {
								return;
							}
							
							final List<ExecutionVertexDeploymentOption> vertexDeploymentOptions = SchedulingStrategyUtils.createExecutionVertexDeploymentOptions(regionVerticesSorted.get(region), id -> deploymentOption);
							// commit scheduling decisions. 提交规划,申请资源 ; 实现类还是 DefaultScheduler
							schedulerOperations.allocateSlotsAndDeploy(vertexDeploymentOptions); { // DefaultScheduler.allocateSlotsAndDeploy()
								
								transitionToScheduled(verticesToDeploy);
								List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = allocateSlots(executionVertexDeploymentOptions);
								List<DeploymentHandle> deploymentHandles = createDeploymentHandles(requiredVersionByVertex, deploymentOptionsByVertex);
								
								waitForAllSlotsAndDeploy(deploymentHandles);
							}
							
						}
					}
					
				}
			}
		}
	}
	

}



// 2.2 flink115 自适应,自动扩缩容调度器: AdaptiveScheduler,  其执行调度流程源码; 

class AdaptiveScheduler {
	
	// AdaptiveScheduler.startScheduling();
	AdaptiveScheduler.startScheduling();{
		state.as(Created.class).startScheduling();{
			Context: 实现类还是 AdaptiveScheduler, 其包括各种接口: Context extends StateTransitions.ToFinished, StateTransitions.ToWaitingForResources 
			context.goToWaitingForResources(); {// AdaptiveScheduler.goToWaitingForResources()
				ResourceCounter desiredResources = calculateDesiredResources();
				transitionToState(new WaitingForResources.Factory());
				
			}
		}
	}
	
	
	
}







