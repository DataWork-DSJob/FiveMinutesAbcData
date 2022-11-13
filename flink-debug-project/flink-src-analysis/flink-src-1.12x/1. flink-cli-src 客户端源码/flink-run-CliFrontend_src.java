
/** 1 flinkrun: CliFrontend 启动
	CliFrontend.main()
		cli.parseParameters(args);
			switch (action) {
				case ACTION_RUN:
			}
*/

// flink-client_1.9
/**
	CliFrontend.main()
		cli.parseParameters(args);
			
*/
CliFrontend.main(){
	final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
	int retCode = SecurityUtils.getInstalledContext().runSecured(
		() -> cli.parseParameters(args){
			switch (action) {
				case ACTION_RUN: run(args);{// CliFrontend.run()
					runProgram(customCommandLine, commandLine, runOptions, program);{
						final ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);{
							// flink-yarn_2.11_1.9.3_src: 联通yarn提交flink作业的 实现;
							FlinkYarnSessionCli.createClusterDescriptor(){
								Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
								return createDescriptor();
							}
							
						}
						
					}
				}
			}
		}
	);
	System.exit(retCode);
}


	

/**flink-client_1.12_src: 
	CliFrontend.main()
		loadCustomCommandLines(configuration, configurationDirectory);
		cli.parseAndRun(args));//CliFrontend.parseAndRun()
			run(params);{//CliFrontend.run()
				
*/
// flink-client_1.12
// 1.1  flink run 识别 execution.target 和相关运行参数, 并创建相应的 Execution对象; 
CliFrontend.main() -> run()
	- validateAndGetActiveCommandLine(commandLine); 	依据是否有 /tmp/.yarn-properties-$user 文件创建Commaon Cli对象; 
	- effectiveConfiguration =getEffectiveConfiguration(commandLine) 解析和创建 执行环境Configuration;
	- executeProgram() -> mainMethod.invoke() 执行 WordCount等应用App的 main()方法
	- env.execute() 开始提交Job执行; 

cli.CliFrontend.main(){
	// 加载多个 命令行, 默认3各: GenericCLI, FlinkYarnSessionCli, DefaultCLI;  
	final List<CustomCommandLine> customCommandLines=loadCustomCommandLines(configuration, configurationDirectory);{
		customCommandLines.add(new GenericCLI(configuration, configurationDirectory));
		customCommandLines.add(loadCustomCommandLine(flinkYarnSessionCLI,configuration,configurationDirectory,"y","yarn"));// "org.apache.flink.yarn.cli.FlinkYarnSessionCli"
		customCommandLines.add(new DefaultCLI());
	}
	final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
	int retCode =SecurityUtils.getInstalledContext().runSecured(() -> cli.parseAndRun(args));{//CliFrontend.parseAndRun()
		String action = args[0];// run/applicaton-run 
		switch (action) {
			case ACTION_RUN: run(params);{//CliFrontend.run()
					final CommandLine commandLine = getCommandLine(commandOptions, args, true);
					activeCommandLine =validateAndGetActiveCommandLine(checkNotNull(commandLine));{
						for (CustomCommandLine cli : customCommandLines) {
							cli.isActive(commandLine){
								GenericCLI.isActive(){return configuration.getOptional(DeploymentOptions.TARGET).isPresent()
									|| commandLine.hasOption(executorOption.getOpt())
									|| commandLine.hasOption(targetOption.getOpt());}
							
								FlinkYarnSessionCli.isActive(){
									if (!super.isActive(commandLine)) {
										boolean isYarnMode = isYarnPropertiesFileMode(commandLine);{
											// 奇怪,只要 args=>commandLine 中不含有 "m" 参数,就是 canApplyYarn就==ture ? 默认都采用 yarn?
											boolean canApplyYarnProperties = !commandLine.hasOption(addressOption.getOpt()); // commandLine.hasOption("m")
											if (canApplyYarnProperties) {
												for (Option option : commandLine.getOptions()) {
													if (!isDetachedOption(option)) {
														canApplyYarnProperties = false;
														break;
													}
												}
											}
											return canApplyYarnProperties;
										}
										// 尝试/tmp/.yarn-properties-bigdata. ($java.io.tmpdir/.yarn-properties-$user/ 目录下查看 存放 ApplicationID 对应的session; 
										File yarnPropertiesLocation = getYarnPropertiesLocation(yarnPropertiesFileLocation);{
											if (yarnPropertiesFileLocation != null) {
												propertiesFileLocation = yarnPropertiesFileLocation;
											}else {
												propertiesFileLocation = System.getProperty("java.io.tmpdir");
											}
											return new File(propertiesFileLocation, YARN_PROPERTIES_FILE + currentUser);
										}
										yarnPropertiesFile.load(new FileInputStream(yarnPropertiesLocation));
										
										final String yarnApplicationIdString =yarnPropertiesFile.getProperty(YARN_APPLICATION_ID_KEY);// 读取applicationID
										yarnApplicationIdFromYarnProperties =ConverterUtils.toApplicationId(yarnApplicationIdString);
										return ( isYarnMode && yarnApplicationIdFromYarnProperties != null);
									}
									return true;
								};
							}
							if (cli.isActive(commandLine)) {
								return cli;
							}
						}
					}
					
					final List<URL> jobJars = getJobJarAndDependencies(programOptions);
					// 定义有效的核心配置,包括 execution.target, 
					Configuration effectiveConfiguration =getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);{
						commandLineConfiguration =activeCustomCommandLine.toConfiguration(commandLine);{//CustomCommandLine.toConfiguration()
							FlinkYarnSessionCli.toConfiguration(){}
							
							DefaultCLI.toConfiguration()
							
							KubernetesSessionCli.toConfiguration(){}
							
						}
						return new Configuration(commandLineConfiguration);
					}
					
					executeProgram(effectiveConfiguration, program);{
						ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);{
							// 把环境变量和各执行上下文 封装进 StreamContextEnvironment
							ContextEnvironment.setAsContext();
							StreamContextEnvironment.setAsContext();
							
							program.invokeInteractiveModeForExecution();{
								mainMethod = entryClass.getMethod("main", String[].class);
								// 执行 App的main()方法,如 WordCount.main()
								mainMethod.invoke(null, (Object) args);{
									// app重点 env.execute()触发各种作业的执行; 源码见 flink-core: ExecutionEnvironment.execute()
									ExecutionEnvironment.execute();
									StreamExecutionEnvironment.execute();
								}
							}
						}
					}
				}
			case ACTION_RUN_APPLICATION: 
				runApplication(params); 
			case ACTION_STOP:	
				stop(params);
				
		}
	}
}

applyCommandLineOptionsToConfiguration:79, AbstractCustomCommandLine (org.apache.flink.client.cli)
createClusterDescriptor:60, DefaultCLI (org.apache.flink.client.cli)
createClusterDescriptor:35, DefaultCLI (org.apache.flink.client.cli)
runProgram:216, CliFrontend (org.apache.flink.client.cli)


// 1.1.1  flink-1.9.x 版 CliFrontend.run(): flink run命令

CliFrontend.main(){
	
	List<CustomCommandLine<?>> customCommandLines = loadCustomCommandLines(Configuration configuration, String configurationDirectory);{
		customCommandLines = new ArrayList<>(2);
		
	}
	
	CliFrontend.run(){
		final CustomCommandLine<?> customCommandLine = getActiveCustomCommandLine(commandLine);
		// 尝试 加载和构造 flink-yarn_xx.jar包中: FlinkYarnSessionCli 对象,如果加载成功则放入到 commandLines中; 
		try{
			customCommandLines.add(loadCustomCommandLine("org.apache.flink.yarn.cli.FlinkYarnSessionCli",configuration,configurationDirectory,"y","yarn"));{
				// 这里异常太诡异了, 如果 flink-yarn(包含再flink-dist中)存在,且相关hadoop+yarn依赖类存在, 才能 forName()成功;
				// 如果 HADOOP_CLASSPATH或者CP中缺乏 yarn,flink-yarn相关类,这里就会失败;
				Class<? extends CustomCommandLine> customCliClass =Class.forName(className).asSubclass(CustomCommandLine.class);
				Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);
				return constructor.newInstance(params);
			}
		}catch (NoClassDefFoundError | Exception e) {
			LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
		}
		
		
		runProgram(customCommandLine, commandLine, runOptions, program);{
			
			ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);{
				// 对于yarn 模式人物, 
				FlinkYarnSessionCli.createClusterDescriptor(CommandLine commandLine){
					Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
					return createDescriptor();
				}
				
				// 对于默认的
				DefaultCLI.createClusterDescriptor(CommandLine commandLine){
					// 默认CmdLine是解析 -m(--jobmanager) 成 host:port格式;
					Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);{//AbstractCustomCommandLine.
						Configuration resultingConfiguration = new Configuration(configuration);
						if (commandLine.hasOption(addressOption.getOpt())) {
							String addressWithPort = commandLine.getOptionValue(addressOption.getOpt());
							InetSocketAddress jobManagerAddress = ClientUtils.parseHostPortAddress(addressWithPort);
						}
						return resultingConfiguration;
					}
					
					return new StandaloneClusterDescriptor(effectiveConfiguration);
				}
				
			}
			
			//为空的话, clusterDescriptor 新启动1个集群;
			final ClusterClient<T> client;
			if (clusterId == null && runOptions.getDetachedMode()){
				client = clusterDescriptor.deployJobCluster();
				client.shutdown();
			}else {
				if (clusterId != null) {
					client = clusterDescriptor.retrieve(clusterId);
				}else{
					client = clusterDescriptor.deploySessionCluster(clusterSpecification);
				}
			}
			executeProgram(program, client, userParallelism);
		}
	}
	

}


CliFrontend.main()
	loadCustomCommandLines(configuration, configurationDirectory);
	cli.parseAndRun(args));//CliFrontend.parseAndRun()
		run(params);{//CliFrontend.run()
			FlinkYarnSessionCli.createClusterDescriptor()
				getClusterDescriptor(configuration);//FlinkYarnSessionCli.getClusterDescriptor()
					yarnClient.init(yarnConfiguration);{// YarnClientImpl的父类 AbstractService.init()
						serviceInit(config);// YarnClientImpl.serviceInit()
							timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
								TimelineUtils.buildTimelineTokenService(conf);
									YarnJacksonJaxbJsonProvider.configObjectMapper(mapper): 
										// 报错原因
										YarnJacksonJaxbJsonProvider 类实现的就是 javax.ws.rs.ext.MessageBodyReader, 位于包 jersey-core-1.9.jar中;
										jersey-core-1.9.jar 存在 jax/lib, $HADOOP_HOME/share/hadoop/xx/lib中, 而flink-shaded-hadoop-2.jar胖包中,并没有; 
								

/** flik-yarn_2.11_1.9.1 src: 
	FlinkYarnSessionCli.createClusterDescriptor()
		getClusterDescriptor(configuration);//FlinkYarnSessionCli.getClusterDescriptor()
			yarnClient.init(yarnConfiguration);{// YarnClientImpl的父类 AbstractService.init()
				serviceInit(config);// YarnClientImpl.serviceInit()
					timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
						TimelineUtils.buildTimelineTokenService(conf);
							YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
							
								YarnJacksonJaxbJsonProvider 类实现的就是 javax.ws.rs.ext.MessageBodyReader, 位于包 jersey-core-1.9.jar中;
								jersey-core-1.9.jar 存在 jax/lib, $HADOOP_HOME/share/hadoop/xx/lib中, 而flink-shaded-hadoop-2.jar胖包中,并没有; 
								

*/

FlinkYarnSessionCli.createClusterDescriptor(CommandLine commandLine){
	Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
	return createDescriptor();{
		AbstractYarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor(configuration);{//FlinkYarnSessionCli.getClusterDescriptor()
			// YarnClient /YarnClientImpl 位于 hadoop-yarn-client-version.jar中, 
			// flink-shaded-hadoop-2.jar的胖包中也会有;
			YarnClient yarnClient = YarnClient.createYarnClient();{
				YarnClient client = new YarnClientImpl();
				return client;
			}
			//YarnClientImpl extends YarnClient[ extends AbstractService ], 
			yarnClient.init(yarnConfiguration);{// YarnClientImpl的父类 AbstractService.init()
				setConfig(conf);
				serviceInit(config);
			}
			
			yarnClient.start();
			return new YarnClusterDescriptor();
		}
		
		// Jar Path
		final Path localJarPath;
		if (cmd.hasOption(flinkJar.getOpt())) {
			localJarPath = new Path(userPath);
		}
		yarnClusterDescriptor.setLocalJarPath(localJarPath);
		yarnClusterDescriptor.addShipFiles(shipFiles);
		return yarnClusterDescriptor;
	}
}



// hadoop-yarn-client_2.8.4
YarnClientImpl[父类AbstractService].init(yarnConfiguration);{// YarnClientImpl的父类 AbstractService.init()
	setConfig(conf);
	serviceInit(config);{// YarnClientImpl.serviceInit()
		asyncApiPollIntervalMillis =conf.getLong(YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS,YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
		submitPollIntervalMillis = conf.getLong(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS, YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
		
		if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
			timelineServiceEnabled = true;
			timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
			// 报错就是这里: NoClassDefFoundError: javax/ws/rs/ext/MessageBodyReader
			timelineService = TimelineUtils.buildTimelineTokenService(conf);{
				static { // 静态方法, 加载类 YarnJacksonJaxbJsonProvider
					mapper = new ObjectMapper();
					// YarnJacksonJaxbJsonProvider 类实现的就是 javax.ws.rs.ext.MessageBodyReader, 位于包 jersey-core-1.9.jar中;
					// 
					YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);{
						class YarnJacksonJaxbJsonProvider extends JacksonJaxbJsonProvider
							JacksonJaxbJsonProvider extends JacksonJsonProvider 
								JacksonJsonProvider implements MessageBodyReader<Object>, MessageBodyWriter<Object>, Versioned
						
					}
				}
			}
		}
	
	
	}
}





















/** 2	env.execute() 提交执行
*
*/





// 2.1 env.execute() 触发作业执行:  env.execute() : 创建相应Factory和Executor,生产上下文, submittJob()提交执行; 
env.execute()
	- executorServiceLoader.getExecutorFactory() 通过 加载和比较所有的 PipelineExecutorFactory.name()是否==  execution.target
	- PipelineExecutorFactory.getExecutor() 创建相应 PipelineExecutor实现类: YarnSession,YarnPerJob, KubernetesExecutor,LocalExecutor 等; 
	- PipelineExecutor.execute() 提交执行相应的job作业; 


ExecutionEnvironment.execute(){
	// Streaming 的执行
	StreamExecutionEnvironment.execute(){
		return execute(getStreamGraph(jobName));{
			final JobClient jobClient = executeAsync(streamGraph);
			jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));
			return jobExecutionResult;
		}
	}
	// 分本地执行环境和 远程执行环境
	LocalStreamEnvironment.execute(){
		return super.execute(streamGraph);
	}
	
	RemoteStreamEnvironment.execute(){
		
	}
	StreamContextEnvironment.execute(){};
	StreamPlanEnvironment.execute();{}// ? strema sql ?
	
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



// 2.2.1 JobManager模块, 响应JobSubmit请求的逻辑: submitJob(): RestClient.sendRequest() -> 






