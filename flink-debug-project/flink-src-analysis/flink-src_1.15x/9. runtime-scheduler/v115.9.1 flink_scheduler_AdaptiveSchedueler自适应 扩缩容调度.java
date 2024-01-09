


// 由 JobVertex -> ExecutionVertex 作业图生产执行图, 最核心逻辑: 每个并发建一个exeVertex:
// new JobMaster()->createScheduler()-> new DefaultScheduler()->DefaultExecutionGraphBuilder.buildGraph()->attachJobGraph() ->initializeJobVertex()
ExecutionJobVertex.initialize() {
	for (int i = 0; i < this.parallelismInfo.getParallelism(); i++) {
		ExecutionVertex vertex = new ExecutionVertex(this, i);
		this.taskVertices[i] = vertex;
	}
}


// 1. 实例化 JobMaster-> DefaultScheduler时构建执行图 ExecutionGraphBuilder.buildGraph(): ExecutionGraph
// 1.2 不同调度器Factory 创建相应 SchedulerNg的 instance 
// 由 JobVertex -> ExecutionVertex 作业图生产执行图, 最核心逻辑: 每个并发建一个exeVertex: for (int i = 0; i < this.parallelismInfo.getParallelism(); i++) { new ExecutionVertex(this, i);}

// 在JobMaster服务创建时, 实例化 Scheduler的对象: DefaultScheduler, AdaptiveScheduler, AdaptiveBatchScheduler
new JobMaster(new DefaultExecutionDeploymentTracker());{//new JobMaster() 构造函数中
		resourceManagerLeaderRetriever =highAvailabilityServices.getResourceManagerLeaderRetriever();
		this.schedulerNG = createScheduler(executionDeploymentTracker, jobManagerJobMetricGroup);{//DefaultSlotPoolServiceSchedulerFactory.createScheduler()
			return schedulerNGFactory.createInstance();{//DefaultSchedulerFactory.
				
				// 默认的 NG调度, // ng 
				DefaultSchedulerFactory.createInstance();{
					DefaultSchedulerComponents schedulerComponents =createSchedulerComponents();
					restartBackoffTimeStrategy =RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory().create();
					return new DefaultScheduler();{super(){// SchedulerBase构造方法, 构建执行计划Graph
						this.executionGraph =createAndRestoreExecutionGraph();{
							ExecutionGraph newExecutionGraph = executionGraphFactory.createAndRestoreExecutionGraph(); {
								// 从JobGraph -> ExecutionGraph执行图, 核心的构建转换方法 
								ExecutionGraph newExecutionGraph = DefaultExecutionGraphBuilder.buildGraph();{
									executionGraph = new DefaultExecutionGraph();
									executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
									
									 // initialize the vertices that have a master initialization hook
									// file output formats create directories here, input formats create splits
									for (JobVertex vertex : jobGraph.getVertices()) {
										vertex.initializeOnMaster(new SimpleInitializeOnMasterContext());
									}
									// topologically sort the job vertices and attach the graph to the existing one
									executionGraph.attachJobGraph(sortedTopology); {//
										        if (isDynamic) {
													attachJobGraph(topologicallySorted, Collections.emptyList());
												} else {// 默认false, 
													attachJobGraph(topologicallySorted, topologicallySorted); {
														
														attachJobVertices(verticesToAttach);
														
														initializeJobVertices(verticesToInitialize);{
															for (JobVertex jobVertex : topologicallySorted) {
																// 应该是由上面的 attachJobVertices()先生产OneByOne的 ExeJobVertex,
																final ExecutionJobVertex ejv = tasks.get(jobVertex.getID());
																initializeJobVertex(ejv, createTimestamp); {
																	ejv.initialize();{//ExecutionJobVertex.initialize()
																		this.taskVertices = new ExecutionVertex[parallelismInfo.getParallelism()];
																		this.inputs = new ArrayList<>(jobVertex.getInputs().size());
																		for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
																			IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);
																			this.producedDataSets[i] =  new IntermediateResult();
																		}
																		// 核心,每个并发一个 SubTaskVertex, // create all task vertices
																		for (int i = 0; i < this.parallelismInfo.getParallelism(); i++) {
																			ExecutionVertex vertex = new ExecutionVertex(this, i);
																			this.taskVertices[i] = vertex;
																		}
																	}
																}
															}
														}
														
														// the topology assigning should happen before notifying new vertices to failoverStrategy
														executionTopology = DefaultExecutionTopology.fromExecutionGraph(this);
														partitionGroupReleaseStrategy = partitionGroupReleaseStrategyFactory.createInstance(getSchedulingTopology());
													}
												}
									}
									
									StateBackend rootBackend =  StateBackendLoader.fromApplicationOrConfigOrDefault();
									
									CheckpointStorage rootStorage =  CheckpointStorageLoader.load();
									executionGraph.enableCheckpointing();
									return executionGraph;
								}
							}
							
							newExecutionGraph.start(mainThreadExecutor);
							return newExecutionGraph;
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


// f115_runtime_scheduler 
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






MiniCluster.submitJob() ->  Dispatcher.submitJob()
	internalSubmitJob:532, Dispatcher 
	waitForTerminatingJob:1285, Dispatcher
		runJob:596, Dispatcher 
		 start:447, EmbeddedLeaderService$EmbeddedLeaderElectionService
		  execute:429, EmbeddedLeaderService 
			run:557, EmbeddedLeaderService$GrantLeadershipCall
			 grantLeadership:249, JobMasterServiceLeadershipRunner
				startJobMasterServiceProcessAsync:257, JobMasterServiceLeadershipRunner 
				 verifyJobSchedulingStatusAndCreateJobMasterServiceProcess:277, JobMasterServiceLeadershipRunner
				 createNewJobMasterServiceProcess:309, JobMasterServiceLeadershipRunner 
				 create:56, DefaultJobMasterServiceProcessFactory
					createJobMasterService:92, DefaultJobMasterServiceFactory
						internalCreateJobMasterService:124, DefaultJobMasterServiceFactory 
							onStart:388, JobMaster 
							 startJobMasterServices:943, JobMaster 
							 start:488, EmbeddedLeaderService$EmbeddedLeaderRetrievalService 
							 addListener:346, EmbeddedLeaderService
							 notifyListener:324, EmbeddedLeaderService 
							

newResourcesAvailable:418, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
notifyNewSlotsAreAvailable:-1, 1737232457 (org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler$$Lambda$708)
internalOfferSlots:229, DefaultDeclarativeSlotPool (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:187, DefaultDeclarativeSlotPool (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:180, DeclarativeSlotPoolService (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:646, JobMaster (org.apache.flink.runtime.jobmaster)
invoke0:-1, NativeMethodAccessorImpl (sun.reflect)
createExecutionGraphWithAvailableResources:178, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
checkDesiredOrSufficientResourcesAvailable:157, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
run:-1, 219907067 (org.apache.flink.runtime.scheduler.adaptive.WaitingForResources$$Lambda$790)
runIfState:1133, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
lambda$runIfState$26:1148, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
run:-1, 2146911571 (org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler$$Lambda$739)
call:511, Executors$RunnableAdapter (java.util.concurrent)
run$$$capture:266, FutureTask (java.util.concurrent)
run:-1, FutureTask (java.util.concurrent)
 - Async stack trace
<init>:151, FutureTask (java.util.concurrent)

schedule:425, RpcEndpoint$MainThreadExecutor (org.apache.flink.runtime.rpc)
runIfState:1147, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
checkDesiredOrSufficientResourcesAvailable:160, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
notifyNewResourcesAvailable:142, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
tryRun:107, State (org.apache.flink.runtime.scheduler.adaptive)
newResourcesAvailable:418, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
internalOfferSlots:229, DefaultDeclarativeSlotPool (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:187, DefaultDeclarativeSlotPool (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:180, DeclarativeSlotPoolService (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:646, JobMaster (org.apache.flink.runtime.jobmaster)
invoke0:-2, NativeMethodAccessorImpl (sun.reflect)



JobMaster.offerSlots(taskManagerId, slots, timeout) {
	TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.get(taskManagerId);
	// slotPoolService: SlotPoolService, flink115有2中实现 提供slots, 
	return CompletableFuture.completedFuture(slotPoolService.offerSlots()); { 
		
		//Adaptive模式, 是 DeclarativeSlotPoolBridge 继承了 DefaultDeclarativeSlotPool
		DeclarativeSlotPoolBridge.offerSlots(){
			if (isJobRestarting) {
				
			} else {
				return getDeclarativeSlotPool().offerSlots();{//DefaultDeclarativeSlotPool.offerSlots()
					return internalOfferSlots(offers);{//DefaultDeclarativeSlotPool.internalOfferSlots()
						Collection<AllocatedSlot> acceptedSlots = new ArrayList<>();
						for (SlotOffer offer : offers) {
							if (slotPool.containsSlot(offer.getAllocationId())) {
								// we have already accepted this offer
								acceptedSlotOffers.add(offer);
							} else {
								Optional<AllocatedSlot> acceptedSlot = matchOfferWithOutstandingRequirements();{
									Optional<ResourceProfile> match = matchingCondition.apply(slotOffer.getResourceProfile());
									if (match.isPresent()) {
										ResourceProfile matchedRequirement = match.get();
										increaseAvailableResources(ResourceCounter.withResource(matchedRequirement, 1));
										AllocatedSlot allocatedSlot = createAllocatedSlot(slotOffer, taskManagerLocation, taskManagerGateway);
										return Optional.of(allocatedSlot);
									}
								}
								// 获取Slot, 如果获取成功 就加到 acceptedSlotOffers 里面; 
								if (acceptedSlot.isPresent()) {
									acceptedSlotOffers.add(offer);
									acceptedSlots.add(acceptedSlot.get());
								} else {
									LOG.debug( "Could not match offer {} to any outstanding requirement.", offer.getAllocationId());
								}
							}
						}
						
						slotPool.addSlots(acceptedSlots, currentTime);
						if (!acceptedSlots.isEmpty()) {
							newSlotsListener.notifyNewSlotsAreAvailable(acceptedSlots);{
								// WaitingForResources.tryRun()
								state.tryRun(ResourceConsumer::notifyNewResourcesAvailable); {
									WaitingForResources.notifyNewResourcesAvailable() {
										// 递归方法, 
										checkDesiredOrSufficientResourcesAvailable();{//WaitingForResources.checkDesiredOrSufficientResourcesAvailable()
											
											if (context.hasSufficientResources()) {
												// Determines whether the deadline is in the past, i.e. whether the time left is zero or negative.
												if (resourceStabilizationDeadline.isOverdue()) {
													// 这里才是创建 执行图, 调AdaptiveScheduler.goToCreatingExecutionGraph() 
													createExecutionGraphWithAvailableResources(); {//WaitingForResources.createExecutionGraphWithAvailableResources()
														context.goToCreatingExecutionGraph();{//AdaptiveScheduler.goToCreatingExecutionGraph()
															executionGraphWithAvailableResourcesFuture = createExecutionGraphWithAvailableResourcesAsync(); {//AdaptiveScheduler.
																
																vertexParallelism = determineParallelism(slotAllocator);
																adjustedParallelismStore = computeVertexParallelismStoreForExecution();
																// 先异步创建 StateAync状态同步器, 
																CompletableFuture<ExecutionGraph> executionGraphFuture = createExecutionGraphAndRestoreStateAsync(adjustedParallelismStore);{
																	backgroundTask.abort();
																	backgroundTask = backgroundTask.runAfter(() -> createExecutionGraphAndRestoreState(adjustedParallelismStore), ioExecutor);
																	return FutureUtils.switchExecutor();
																}
																// 
																return executionGraphFuture.thenApply(CreatingExecutionGraph.ExecutionGraphWithVertexParallelism.create());
															}
														}
													}
												} else {
													 context.runIfState(this::checkDesiredOrSufficientResourcesAvailable);
												}
											}
										}
									}
								}
							}
						}
						return acceptedSlotOffers;
					}
					
				}
			}
	
		}
		
		// 默认 Default 
		DefaultDeclarativeSlotPool.offerSlots() {
			
		}
		
		
	}
}


createExecutionGraphWithAvailableResourcesAsync:962, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
goToCreatingExecutionGraph:923, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
createExecutionGraphWithAvailableResources:178, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
checkDesiredOrSufficientResourcesAvailable:157, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
run:-1, 698670189 (org.apache.flink.runtime.scheduler.adaptive.WaitingForResources$$Lambda$770)
runIfState:1133, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
lambda$runIfState$26:1148, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
run:-1, 1736046527 (org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler$$Lambda$730)
call:511, Executors$RunnableAdapter (java.util.concurrent)
run$$$capture:266, FutureTask (java.util.concurrent)
run:-1, FutureTask (java.util.concurrent)
 - Async stack trace
<init>:151, FutureTask (java.util.concurrent)
schedule:425, RpcEndpoint$MainThreadExecutor (org.apache.flink.runtime.rpc)
runIfState:1147, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
checkDesiredOrSufficientResourcesAvailable:160, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
notifyNewResourcesAvailable:142, WaitingForResources (org.apache.flink.runtime.scheduler.adaptive)
tryRun:107, State (org.apache.flink.runtime.scheduler.adaptive)
newResourcesAvailable:418, AdaptiveScheduler (org.apache.flink.runtime.scheduler.adaptive)
internalOfferSlots:229, DefaultDeclarativeSlotPool (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:187, DefaultDeclarativeSlotPool (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:180, DeclarativeSlotPoolService (org.apache.flink.runtime.jobmaster.slotpool)
offerSlots:646, JobMaster (org.apache.flink.runtime.jobmaster)
invoke0:-2, NativeMethodAccessorImpl (sun.reflect)
invoke:62, NativeMethodAccessorImpl (sun.reflect)



/* 
* 触发来源: JobMaster.offerSlots()
* 位于 JM线程: flink-akka.actor.default-dispatcher-8, 
*/ 

offerSlots:646, JobMaster 
 internalOfferSlots:229, DefaultDeclarativeSlotPool
	notifyNewSlotsAreAvailable AdaptiveScheduler$$Lambda$723
		newResourcesAvailable:418, AdaptiveScheduler 
			notifyNewResourcesAvailable:142, WaitingForResources
				checkDesiredOrSufficientResourcesAvailable:147, WaitingForResources 
				 createExecutionGraphWithAvailableResources:178
				  goToCreatingExecutionGraph:923, AdaptiveScheduler
				    createExecutionGraphWithAvailableResourcesAsync:962, AdaptiveScheduler
					 return createExecutionGraphAndRestoreStateAsync(adjustedParallelismStore)
						.thenApply(CreatingExecutionGraph.ExecutionGraphWithVertexParallelism.create());
	
// 由 远程TM触发, 
// 远程JM端, JobMaster.offerSlots() -> DefaultDeclarativeSlotPool.internalOfferSlots() -> DefaultScheduler.assignResource() -> deployAll()
JobMaster.offerSlots() -> DefaultDeclarativeSlotPool.internalOfferSlots()
	internalOfferSlots:229, DefaultDeclarativeSlotPool
		newSlotsAreAvailable:232, DeclarativeSlotPoolBridge
		
		for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
			PendingRequest pendingRequest = requestSlotMatch.getPendingRequest();
			pendingRequest.fulfill(slot); {
				DefaultScheduler.assignResource();
				
				DefaultScheduler.registerProducedPartitions() {
					
				}
				
				DefaultScheduler.deployAll(List<DeploymentHandle> deploymentHandles) {
					
				}
			}
			
		}



/* 
* 触发来源: 
* 位于 JM线程: jobmanager-io-thread-2, 
*/ 

// backgroundTask: 执行完后, 就执行 createExecutionGraphAndRestoreState()方法; 
AdaptiveScheduler.createExecutionGraphAndRestoreStateAsync() {
	backgroundTask.abort();//BackgroundTask
	backgroundTask = backgroundTask.runAfter(
		() -> createExecutionGraphAndRestoreState(adjustedParallelismStore),ioExecutor); {
			return executionGraphFactory.createAndRestoreExecutionGraph();{
				executionDeploymentListener = new ExecutionDeploymentTrackerDeploymentListenerAdapter();
				ExecutionGraph newExecutionGraph = DefaultExecutionGraphBuilder.buildGraph(); 
			}
		}
		
   return FutureUtils.switchExecutor(backgroundTask.getResultFuture(), getMainThreadExecutor());
}

	// 基于作业计划(JobGraph)构建执行计划(ExecutionGraph) 的源码; 
	// 最核心逻辑: 每个并发建一个exeVertex: for (int i = 0; i < this.parallelismInfo.getParallelism(); i++) { new ExecutionVertex(this, i);}
	ExecutionGraph newExecutionGraph = executionGraphFactory.createAndRestoreExecutionGraph(); {
		// 从JobGraph -> ExecutionGraph执行图, 核心的构建转换方法 
		ExecutionGraph newExecutionGraph = DefaultExecutionGraphBuilder.buildGraph();{
			executionGraph = new DefaultExecutionGraph();
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
			
			 // initialize the vertices that have a master initialization hook
			// file output formats create directories here, input formats create splits
			for (JobVertex vertex : jobGraph.getVertices()) {
				vertex.initializeOnMaster(new SimpleInitializeOnMasterContext());
			}
			// topologically sort the job vertices and attach the graph to the existing one
			executionGraph.attachJobGraph(sortedTopology); {//
						if (isDynamic) {
							attachJobGraph(topologicallySorted, Collections.emptyList());
						} else {// 默认false, 
							attachJobGraph(topologicallySorted, topologicallySorted); {
								
								attachJobVertices(verticesToAttach);
								
								initializeJobVertices(verticesToInitialize);{
									for (JobVertex jobVertex : topologicallySorted) {
										// 应该是由上面的 attachJobVertices()先生产OneByOne的 ExeJobVertex,
										final ExecutionJobVertex ejv = tasks.get(jobVertex.getID());
										initializeJobVertex(ejv, createTimestamp); {
											ejv.initialize();{//ExecutionJobVertex.initialize()
												this.taskVertices = new ExecutionVertex[parallelismInfo.getParallelism()];
												this.inputs = new ArrayList<>(jobVertex.getInputs().size());
												for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
													IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);
													this.producedDataSets[i] =  new IntermediateResult();
												}
												// 核心,每个并发一个 SubTaskVertex, // create all task vertices
												for (int i = 0; i < this.parallelismInfo.getParallelism(); i++) {
													ExecutionVertex vertex = new ExecutionVertex(this, i);
													this.taskVertices[i] = vertex;
												}
											}
										}
									}
								}
								
								// the topology assigning should happen before notifying new vertices to failoverStrategy
								executionTopology = DefaultExecutionTopology.fromExecutionGraph(this);
								partitionGroupReleaseStrategy = partitionGroupReleaseStrategyFactory.createInstance(getSchedulingTopology());
							}
						}
			}
			
			StateBackend rootBackend =  StateBackendLoader.fromApplicationOrConfigOrDefault();
			
			CheckpointStorage rootStorage =  CheckpointStorageLoader.load();
			executionGraph.enableCheckpointing();
			return executionGraph;
		}
	}
	


DefaultScheduler.waitForAllSlotsAndDeploy() {
	deploymentHandle.getSlotExecutionVertexAssignment()
		.getLogicalSlotFuture()
		.handle(assignResource(deploymentHandle))
		.thenCompose(registerProducedPartitions(deploymentHandle))
		.handle(() -> handleTaskDeploymentFailure());
		
	deployAll(deploymentHandles);
}



















