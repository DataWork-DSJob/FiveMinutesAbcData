

// flink_1.15_src

Dispatcher.submitJob(){
	if (isDuplicateJob(jobGraph.getJobID())) {
		
	} else if (isPartialResourceConfigured(jobGraph)) {
		return FutureUtils.completedExceptionally(new JobSubmissionException());
	} else {// 正常进入这里; 提交Job
		return internalSubmitJob(jobGraph);{//Dispatcher.internalSubmitJob()
			
			return waitForTerminatingJob(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
                .handle((ignored, throwable) -> handleTermination(jobGraph.getJobID(), throwable))
                .thenCompose(Function.identity());{
				// 在waitForTerminatingJob()方法中,执行 Dispatcher.persistAndRunJob()
				Dispatcher.persistAndRunJob(){
					jobGraphWriter.putJobGraph(jobGraph);
					JobManagerRunner jobManagerRunner = createJobMasterRunner(jobGraph);{//Dispatcher.createJobMasterRunner
						return jobManagerRunnerFactory.createJobManagerRunner();{//JobMasterServiceLeadershipRunnerFactory.createJobManagerRunner()
							
							final JobMasterConfiguration jobMasterConfiguration =JobMasterConfiguration.fromConfiguration(configuration);
							final JobResultStore jobResultStore = highAvailabilityServices.getJobResultStore();
							final LeaderElectionService jobManagerLeaderElectionService = highAvailabilityServices.getJobManagerLeaderElectionService(jobGraph.getJobID());
							
							// 这里定义Scheduler, 生成SchedulerFactory; 并checkState()
							final SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory = DefaultSlotPoolServiceSchedulerFactory
								.fromConfiguration(configuration, jobGraph.getJobType());
							if (jobMasterConfiguration.getConfiguration().get(JobManagerOptions.SCHEDULER_MODE) == SchedulerExecutionMode.REACTIVE) {
								Preconditions.checkState(slotPoolServiceSchedulerFactory.getSchedulerType() == JobManagerOptions.SchedulerType.Adaptive,
										"Adaptive Scheduler is required for reactive mode");
							}

							final LibraryCacheManager.ClassLoaderLease classLoaderLease =jobManagerServices.getLibraryCacheManager().registerClassLoaderLease(jobGraph.getJobID());
							final DefaultJobMasterServiceFactory jobMasterServiceFactory = new DefaultJobMasterServiceFactory(jobManagerServices.getIoExecutor(),rpcService,);
							final DefaultJobMasterServiceProcessFactory jobMasterServiceProcessFactory = new DefaultJobMasterServiceProcessFactory();
							
							return new JobMasterServiceLeadershipRunner( jobMasterServiceProcessFactory,jobManagerLeaderElectionServicer);

						}
					}
					runJob(jobManagerRunner, ExecutionType.SUBMISSION);{//Dispatcher.runJob()
						
					}
				}
			}
			
		}
	}
}


DefaultJobMasterServiceFactory.createJobMasterService() {
	// 异步线程执行 new JobMaster服务; 
	return CompletableFuture.supplyAsync(FunctionUtils.uncheckedSupplier(
		() -> internalCreateJobMasterService(leaderSessionId, onCompletionActions)), executor);{//DefaultJobMasterServiceFactory.internalCreateJobMasterService()
			// 运行1个Job最重要的初始化?
			JobMaster jobMaster = new JobMaster();
			// 启动运行?
			jobMaster.start();{
				this.rpcServer.start();
			}
			return jobMaster;
		}
}



new JobMaster(new DefaultExecutionDeploymentTracker());{//new JobMaster() 构造函数中
		resourceManagerLeaderRetriever =highAvailabilityServices.getResourceManagerLeaderRetriever();
		
		this.schedulerNG = createScheduler(executionDeploymentTracker, jobManagerJobMetricGroup);{
			return schedulerNGFactory.createInstance();{//DefaultSchedulerFactory.
				
				// 自适应调度NGFactory: 
				AdaptiveSchedulerFactory.createInstance();{
					SlotSharingSlotAllocator slotAllocator = createSlotSharingSlotAllocator(declarativeSlotPool);
					return new AdaptiveScheduler();
				}
				// 默认的 NG调度
				DefaultSchedulerFactory.createInstance();{
					DefaultSchedulerComponents schedulerComponents =createSchedulerComponents();
					restartBackoffTimeStrategy =RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory().create();
					return new DefaultScheduler();{super(){// SchedulerBase构造方法, 构建执行计划Graph
						this.executionGraph =createAndRestoreExecutionGraph();{
							ExecutionGraph newExecutionGraph =createExecutionGraph();{//SchedulerBase.
								// 核心步骤,构建 物理执行计划
								return ExecutionGraphBuilder.buildGraph();{//ExecutionGraphBuilder.buildGraph()
									JobInformation jobInformation =new JobInformation();
									executionGraph.attachJobGraph(sortedTopology);
									ExecutionGraph executionGraph =new ExecutionGraph();
									executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
									for (JobVertex vertex : jobGraph.getVertices()) {
										vertex.initializeOnMaster(classLoader);
									}
									List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
									executionGraph.attachJobGraph(sortedTopology);{
										ExecutionJobVertex ejv =new ExecutionJobVertex();{
											List<SerializedValue<OperatorCoordinator.Provider>> coordinatorProviders =getJobVertex().getOperatorCoordinators();
											for (final SerializedValue<OperatorCoordinator.Provider> provider :coordinatorProviders) {
												OperatorCoordinatorHolder.create(provider, this, graph.getUserClassLoader());{
													TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader);
													OperatorCoordinator.Provider provider =serializedProvider.deserializeValue(classLoader);
													
												}
										}
										
									}
									
									CheckpointIDCounter checkpointIdCounter= recoveryFactory.createCheckpointIDCounter(jobId);
									CheckpointStatsTracker checkpointStatsTracker =new CheckpointStatsTracker();
									rootBackend =StateBackendLoader.fromApplicationOrConfigOrDefault();
									
								}
							}
							CheckpointCoordinator checkpointCoordinator =newExecutionGraph.getCheckpointCoordinator();
						}
						inputsLocationsRetriever =new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);
						this.coordinatorMap = createCoordinatorMap();
					}}
					}
				}
		}
	}
}











Job的资源管理 scheduler生成时机: 
- Client端发出submitJob请求并提交作业后, JM进程接受消息并执行 submitJob()方法; 

// JM进程, Akka消息路由Dispatcher
Dispatcher.submitJob() -> Dispatcher.internalSubmitJob()
	waitForTerminatingJob() -> Dispatcher.persistAndRunJob()
		Dispatcher.createJobMasterRunner() 
			DefaultSlotPoolServiceSchedulerFactory.fromConfiguration();







