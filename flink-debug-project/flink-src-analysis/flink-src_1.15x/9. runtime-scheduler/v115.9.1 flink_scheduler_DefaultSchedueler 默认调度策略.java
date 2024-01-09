


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


// 2. 1 flink115 默认调度策略 DefaultScheduler, 非自适应策略,  其执行调度流程源码; 

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
								
								waitForAllSlotsAndDeploy(deploymentHandles);{// DefaultScheduler.waitForAllSlotsAndDeploy()
									// 绑定所有分区后, 指定deployAll()发布
									//FutureUtils.assertNoException(assignAllResourcesAndRegisterProducedPartitions(deploymentHandles).handle(deployAll(deploymentHandles)));
									CompletableFuture<Void> future = assignAllResourcesAndRegisterProducedPartitions(deploymentHandles){
										for (DeploymentHandle deploymentHandle : deploymentHandles) {
											final CompletableFuture<Void> resultFuture = deploymentHandle.getSlotExecutionVertexAssignment()
													.getLogicalSlotFuture()
													.handle(assignResource(deploymentHandle)) {
														
													}
													.thenCompose(registerProducedPartitions(deploymentHandle))
													.handle((ignore, throwable) -> {
																if (throwable != null) {
																	handleTaskDeploymentFailure(deploymentHandle.getExecutionVertexId(),throwable);
																}
																return null;
															});
											resultFutures.add(resultFuture);
										}
										return FutureUtils.waitForAll(resultFutures);
									}
									CompletableFuture<U> future2 = future.handle(deployAll(deploymentHandles));{//deployAll()
										for (final DeploymentHandle deploymentHandle : deploymentHandles) {
											final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
											final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
											checkState(slotAssigned.isDone());
											
										}
									}
									FutureUtils.assertNoException(future2);
								}
							}
							
						}
					}
					
				}
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
							


// 申请到Slots后, 管理发布资源? 
ResourceManagerServiceImpl.grantLeadership()
	lambda$grantLeadership$2:203, ResourceManagerServiceImpl
		startNewLeaderResourceManager:266, ResourceManagerServiceImpl
			confirmLeadership:459, EmbeddedLeaderService$EmbeddedLeaderElectionService 
			 confirmLeader:267, EmbeddedLeaderService 
			  notifyAllListeners:283, EmbeddedLeaderService
				notifyListener:324, EmbeddedLeaderService
					notifyLeaderAddress:2253, TaskExecutor$ResourceManagerLeaderListener
					
notifyLeaderAddress$0:2255, TaskExecutor$ResourceManagerLeaderListener 
	notifyOfNewResourceManagerLeader:1305, TaskExecutor
		reconnectToResourceManager:1326, TaskExecutor 
			connectToResourceManager:1360, TaskExecutor {
				resourceManagerConnection = new TaskExecutorToResourceManagerConnection();
				resourceManagerConnection.start(); // RegisteredRpcConnection.start()
			}



notifyLeaderAddress$0:2255, TaskExecutor$ResourceManagerLeaderListener 
	notifyOfNewResourceManagerLeader:1305, TaskExecutor
		reconnectToResourceManager:1326, TaskExecutor 
			connectToResourceManager:1360, TaskExecutor {
				resourceManagerConnection = new TaskExecutorToResourceManagerConnection();
				resourceManagerConnection.start(); // RegisteredRpcConnection.start()
			}


JobMaster.ResourceManagerLeaderListener.notifyLeaderAddress(){
	runAsync(() -> notifyOfNewResourceManagerLeader());{//JobMaster.notifyOfNewResourceManagerLeader()
		resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        reconnectToResourceManager(new FlinkException(String.format(
					"ResourceManager leader changed to new address %s",resourceManagerAddress)));{
			closeResourceManagerConnection(cause);
			tryConnectToResourceManager();
				connectToResourceManager(); {//JobMaster.connectToResourceManager()
					resourceManagerConnection = new ResourceManagerConnection();
					resourceManagerConnection.start(); {//ResourceManagerConnection.start()
						RetryingRegistration<F, G, S, R> newRegistration = createNewRegistration();{// RegisteredRpcConnection.createNewRegistration()
							RetryingRegistration<F, G, S, R> newRegistration = checkNotNull(generateRegistration());{
								new RetryingRegistration();
							}
							
							// RetryingRegistration 注册如果成功, 就执行 onRegistrationSuccess() -> TaskExecutor.establishResourceManagerConnection()方法; 
							future.whenCompleteAsync((result,failure)-> {
								if (result.isSuccess()) {
									targetGateway = result.getGateway();
									onRegistrationSuccess(result.getSuccess()); {
										// 这里调Akka 远程触发TM的 TaskExecutor.ResourceManagerRegistrationListener.onRegistrationSuccess() 方法; 
										// 远程TM线程执行; 
										registrationListener.onRegistrationSuccess(this, success); {
											runAsync(establishResourceManagerConnection()); {//TaskExecutor.establishResourceManagerConnection()
												
											}
										}
									}
								} else if (result.isRejection()) {
									onRegistrationRejection(result.getRejection());
								} else {
									throw new IllegalArgumentException(String.format("Unknown retrying registration response: %s.", result));
								}
							} );
							
						}
						
						if (REGISTRATION_UPDATER.compareAndSet(this, null, newRegistration)) {
							newRegistration.startRegistration();{//
								rpcGatewayFuture = rpcService.connect();
								 // upon success, start the registration attempts
								CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync(()-> register(rpcGateway)); {
									
								}
								// upon failure, retry, unless this is cancelled
								rpcGatewayAcceptFuture.whenCompleteAsync();
							}
						} else {
							// concurrent start operation
							newRegistration.cancel();
						}
					}
				}
		}
	}
}

// 由JM进程 new ResourceManagerConnection().start() 触发, 完成了 ResourceManager 的注册后, 向 TM发送 onRegistrationSuccess()事件; 
// TM进程,  监听到 注册(?)成功, ResourceManagerRegistrationListener.onRegistrationSuccess() 
TaskExecutor.ResourceManagerRegistrationListener.onRegistrationSuccess(result.getSuccess()); {
	registrationListener.onRegistrationSuccess(this, success); {
		runAsync(establishResourceManagerConnection()); {//JobMaster.establishResourceManagerConnection()
			
			final ResourceManagerId resourceManagerId = success.getResourceManagerId();
			// verify the response with current connection
			if (resourceManagerConnection != null && Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {
				log.info("JobManager successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

				final ResourceManagerGateway resourceManagerGateway =resourceManagerConnection.getTargetGateway();
				final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();
				establishedResourceManagerConnection = new EstablishedResourceManagerConnection( resourceManagerGateway, resourceManagerResourceId);
				
				// 关键: 
				slotPoolService.connectToResourceManager(resourceManagerGateway);{//DeclarativeSlotPoolService.connectToResourceManager()
					resourceRequirementServiceConnectionManager.connect( resourceRequirements -> 
						resourceManagerGateway.declareRequiredResources( jobMasterId, resourceRequirements, rpcTimeout));
					declareResourceRequirements(declarativeSlotPool.getResourceRequirements()); {
						resourceRequirementServiceConnectionManager.declareResourceRequirements();{
							triggerResourceRequirementsSubmission();
						}
					}
				}
				
				resourceManagerHeartbeatManager.monitorTarget( resourceManagerResourceId, new ResourceManagerHeartbeatReceiver(resourceManagerGateway));
			} else {
				log.debug( "Ignoring resource manager connection to {} because it's duplicated or outdated.", resourceManagerId);
			}
			
		}
	}
}



// 由远程JM触发, 
// 在TM进程中执行如下方法:  与jm连接, 向jm请求offserSlots; 触发jobMasterGateway.offerSlots(); 
DefaultJobLeaderService.JobManagerLeaderListener.JobManagerRegisteredRpcConnection.onRegistrationSuccess() {
	jobLeaderListener.jobManagerGainedLeadership();{//  TaskExecutor.JobLeaderListenerImpl.jobManagerGainedLeadership()
		runAsync(()-> establishJobManagerConnection());{//TaskExecutor.establishJobManagerConnection()
			JobTable.Connection establishedConnection = associateWithJobManager(job, jobManagerResourceID, jobMasterGateway);
			// monitor the job manager as heartbeat target
			jobManagerHeartbeatManager.monitorTarget(jobManagerResourceID, new JobManagerHeartbeatReceiver(jobMasterGateway));
			internalOfferSlotsToJobManager(establishedConnection);{
				currentSlotOfferPerJob.put(jobId, UUID.randomUUID());
				// 远程Akka调用: JM端执行 JobMaster.offerSlots() 
				CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture =jobMasterGateway.offerSlots();
				
				acceptedSlotsFuture.whenCompleteAsync(handleAcceptedSlotOffers(jobId, jobMasterGateway));{
					
				}
			}
		}
	}
}



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

DefaultScheduler.waitForAllSlotsAndDeploy() {
	deploymentHandle.getSlotExecutionVertexAssignment()
		.getLogicalSlotFuture()
		.handle(assignResource(deploymentHandle))
		.thenCompose(registerProducedPartitions(deploymentHandle))
		.handle(() -> handleTaskDeploymentFailure());
		
	deployAll(deploymentHandles);
}





