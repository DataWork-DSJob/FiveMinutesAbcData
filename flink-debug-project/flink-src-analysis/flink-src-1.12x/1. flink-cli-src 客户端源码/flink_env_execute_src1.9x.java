

// flink-core_execute()_1.9x
// 提交作业的源码
StreamExecutionEnvironment.execute(String jobName){
	StreamGraph streamGraph = getStreamGraph(jobName);{
		
	}
	return execute(streamGraph);{
			// 分本地执行环境和 远程执行环境
		LocalStreamEnvironment.execute(StreamGraph streamGraph){
			return super.execute(streamGraph);
		}
		
		// 远程提交
		RemoteStreamEnvironment.execute(){
			
		}
		
	}
}

// Local模式的 env.execute()

LocalStreamEnvironment.execute(StreamGraph streamGraph){
	JobGraph jobGraph = streamGraph.getJobGraph();{//StreamGraph.getJobGraph()
		return getJobGraph(null);{
			return StreamingJobGraphGenerator.createJobGraph(this, jobID);
				return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();{
					setChaining(hashes, legacyHashes, chainedOperatorHashes);
					setPhysicalEdges();
					setSlotSharingAndCoLocation();
					JobGraphGenerator.addUserArtifactEntries(streamGraph.getUserArtifacts(), jobGraph);{
						{ // JobGraphGenerator的静态变量mergeIterationAuxTasks的加载:
							static final boolean mergeIterationAuxTasks = GlobalConfiguration.loadConfiguration(){// GlobalConfiguration.loadConfiguration()
									// $FLINK_CONF_DIR/flink-conf.yaml 读取全局配置; 源码见下面的 flink-core_loadFlinkConfig_src1.9x:
									return loadConfiguration(new Configuration());
								}
								.getBoolean(MERGE_ITERATION_AUX_TASKS_KEY, false);
						}
						
					}
					
				}
		}
	}
	Configuration configuration = new Configuration();
	configuration.addAll(jobGraph.getJobConfiguration());
	configuration.addAll(this.configuration);
	
	int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());
	MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
		.setConfiguration(configuration)
		.setNumSlotsPerTaskManager(numSlotsPerTaskManager)
		.build();
	
	MiniCluster miniCluster = new MiniCluster(cfg);
	miniCluster.start();
	return miniCluster.executeJobBlocking(jobGraph);
}



// flink-core_loadFlinkConfig_src1.9x:  加载Flink全局配置的方法: 解析加载 $FLINK_CONF_DIR/flink-conf.yaml 中kv变量;
GlobalConfiguration.loadConfiguration(Configuration dynamicProperties){
	final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
	if (configDir == null) {
		return new Configuration(dynamicProperties);
	}
	// 2. load the global configuration; 就是先后按"#",':' 对目录下 flink-conf.yaml 切分解析k-v;
	return loadConfiguration(configDir, dynamicProperties); { //loadConfiguration(String configDir, Configuration dynamicProperties) 
		File confDirFile = new File(configDir);
		File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);// 读取 flink-conf.yaml 配置文件;
		if (!yamlConfigFile.exists()) {
			throw new IllegalConfigurationException("The Flink config file '" + yamlConfigFile +"' (" + confDirFile.getAbsolutePath() + ") does not exist.");
		}
		Configuration configuration = loadYAMLResource(yamlConfigFile);{
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			while ((line = reader.readLine()) != null) {
				String[] comments = line.split("#", 2);
				if (conf.length() > 0) {
					String[] kv = conf.split(": ", 2);
					String key = kv[0].trim();
					String value = kv[1].trim();
					config.setString(key, value);
				}
			}
		}
		// 如果动态参数不为空, 还要添加动态参数; 
		if (dynamicProperties != null) {
			configuration.addAll(dynamicProperties);
		}
		return configuration;
	}
}







// 2.2 各大引擎核心调用 Stream模式 异步执行作业
// PipelineExecutor.execute() clusterClient.submitJob(): RestClient.sendRequest() 向远程JobManager进程发送 JobSubmit 请求
StreamExecutionEnvironment.executeAsync(StreamGraph streamGraph);{
	// 这里定义了 执行模式和执行引擎; 主要通过 加载和比较所有的 PipelineExecutorFactory.name()是否==  execution.target
	final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);{//core.DefaultExecutorServiceLoader.
		final ServiceLoader<PipelineExecutorFactory> loader = ServiceLoader.load(PipelineExecutorFactory.class);
		while (loader.iterator().hasNext()) {
			// 根据 execution.target 配置项和 PipelineExecutorFactory.NAME 进行比较,看是否相等; 
			boolean isCompatible = factories.next().isCompatibleWith(configuration);{
				RemoteExecutorFactory.isCompatibleWith(){
					return RemoteExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET)); //execution.target==remote
				}
				LocalExecutorFactory.isCompatibleWith(){ // 看execution.target== local
					return LocalExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
				}
				KubernetesSessionClusterExecutorFactory.isCompatibleWith(){//看execution.target是否== kubernetes-session
					return configuration.get(DeploymentOptions.TARGET).equalsIgnoreCase(KubernetesSessionClusterExecutor.NAME);
				}
				//Yarn的三种部署模式:  yarn-per-job, yarn-session, yarn-application
				YarnSessionClusterExecutorFactory.isCompatibleWith(){ // 看execution.target== yarn-session  
					YarnSessionClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
				}
			}
			if (factory != null && isCompatible) compatibleFactories.add(factories.next());
		}
		if (compatibleFactories.size() > 1) { 
			throw new IllegalStateException("Multiple compatible client factories found for:\n" + configStr + ".");
		}
		if (compatibleFactories.isEmpty()) {
			throw new IllegalStateException("No ExecutorFactory found to execute the application.");
		}
		return compatibleFactories.get(0); // 只能定义1个 PipelineExecutorFactory, 否则报错; 
	}
	CompletableFuture<JobClient> jobClientFuture = executorFactory
		.getExecutor(configuration){//PipelineExecutorFactory.getExecutor()
			LocalExecutorFactory.getExecutor()
			RemoteExecutorFactory.getExecutor()
			EmbeddedExecutorFactory.getExecutor()
			WebSubmissionExecutorFactory.getExecutor()
			
			KubernetesSessionClusterExecutorFactory.getExecutor(){}
			YarnJobClusterExecutorFactory.getExecutor(){}
			YarnSessionClusterExecutorFactory.getExecutor(){
				return new YarnSessionClusterExecutor();
			}
			
		}
		.execute(streamGraph, configuration, userClassloader);{//PipelineExecutor.execute(pipeline,configuration,userCodeClassloader)
			LocalExecutor.execute(){//LocalExecutor.execute()
				final JobGraph jobGraph = getJobGraph(pipeline, effectiveConfig);
				return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory).submitJob(jobGraph);{// PerJobMiniClusterFactory.submitJob()
					MiniCluster miniCluster = miniClusterFactory.apply(miniClusterConfig);
					miniCluster.start();
					
					return miniCluster
						.submitJob(jobGraph){//MiniCluster.submitJob()
							final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = getDispatcherGatewayFuture();
							final CompletableFuture<Void> jarUploadFuture = uploadAndSetJobFiles(blobServerAddressFuture, jobGraph);
							final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture
							.thenCombine(dispatcherGatewayFuture,(Void ack, DispatcherGateway dispatcherGateway) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout)){
								dispatcherGateway.submitJob(): 发起远程Rpc请求: 实际执行 Dispatcher.submitJob()
								Dispatcher.submitJob(){ //远程Rpc调用,并返回结果;
									//代码详情如下:
								}
							}
							.thenCompose(Function.identity());
							return acknowledgeCompletableFuture.thenApply((Acknowledge ignored) -> new JobSubmissionResult(jobGraph.getJobID()));
							
						}
						.thenApply(result -> new PerJobMiniClusterJobClient(result.getJobID(), miniCluster))
						.whenComplete((ignored, throwable) -> {
							if (throwable != null) {
								// We failed to create the JobClient and must shutdown to ensure cleanup.
								shutDownCluster(miniCluster);
							}
						});
						
				}
			}
			
			AbstractJobClusterExecutor.execute();
			
			// YarnCluster, KubeClient 等都是 这个
			AbstractSessionClusterExecutor.execute(){
				final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
				// 判断和创建想要的 Cluster链接端
				ClusterDescriptor clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);{//AbstractSessionClusterExecutor.
					final String configurationDirectory = configuration.get(DeploymentOptionsInternal.CONF_DIR);
					return getClusterDescriptor(configuration);{
						YarnClusterClientFactory.getClusterDescriptor(){
							YarnClient yarnClient = YarnClient.createYarnClient();
							yarnClient.init(yarnConfiguration);
							yarnClient.start();
							return new YarnClusterDescriptor(yarnConfiguration,yarnClient,YarnClientYarnClusterInformationRetriever.create(yarnClient));
						}
						
						kubernetesClusterClientFactory.getClusterDescriptor(){
							
						}
						
					}
				}
				
				ClusterClientProvider<ClusterID> clusterClientProvider =clusterDescriptor.retrieve(clusterID);
				return clusterClient
					.submitJob(jobGraph){// RestClusterClient.submitJob()
						
						Future<JobSubmitResponseBody> submissionFuture = requestFuture.thenCompose(sendRetriableRequest());
						// 创建request: JobSubmitRequestBody
						Tuple2<JobSubmitRequestBody, Collection<FileUpload>> requestFuture= jobGraphFileFuture.thenApply(){
							final JobSubmitRequestBody requestBody =new JobSubmitRequestBody(jobGraphFile.getFileName().toString(),jarFileNames,artifactFileNames);
							return Tuple2.of(requestBody, Collections.unmodifiableCollection(filesToUpload));
						}
						// 发送JobSumbit请求: sendRequest(request)
						submissionFuture= sendRetriableRequest(request);{//RestClusterClient.
							getWebMonitorBaseUrl()
							return restClient.sendRequest(messageParameters,request, filesToUpload);{//RestClient.
								return sendRequest();{//RestClient.sendRequest()
									String targetUrl = MessageParameters.resolveUrl(versionedHandlerURL, messageParameters);// = /v1/jobs
									objectMapper.writeValue(new StringWriter(), request);
									Request httpRequest =createRequest(targetUrl,payload);
									// 这里向集群: bdnode102.hjq.com:36384(YarnSessionClusterEntrypoint) JobManager发送JobSubmitRequest
									return submitRequest(targetAddress, targetPort, httpRequest, responseType);{//RestClient.submitRequest()
										connectFuture = bootstrap.connect(targetAddress, targetPort);
										httpRequest.writeTo(channel);
										future = handler.getJsonFuture();
										parseResponse(rawResponse, responseType);
									}
								}
							}
						}
						
						return submissionFuture.thenApply()
							.exceptionally();
					}
					.thenApplyAsync()
					.thenApplyAsync()
					.whenComplete((ignored1, ignored2) -> clusterClient.close());
			}
			RemoteExecutor[extends AbstractSessionClusterExecutor].execute();
			
			EmbeddedExecutor.execute();
		}
	
	try {
		JobClient jobClient = jobClientFuture.get();
		jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
		return jobClient;
	} catch (ExecutionException executionException) {//执行失败,从这里抛出异常; 
		throw new FlinkException(String.format("Failed to execute job '%s'.", streamGraph.getJobName()),strippedException);
	}
}



