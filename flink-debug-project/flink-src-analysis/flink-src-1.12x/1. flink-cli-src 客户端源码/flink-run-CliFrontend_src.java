
/** 1 flinkrun: CliFrontend ����
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
							// flink-yarn_2.11_1.9.3_src: ��ͨyarn�ύflink��ҵ�� ʵ��;
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
// 1.1  flink run ʶ�� execution.target ��������в���, ��������Ӧ�� Execution����; 
CliFrontend.main() -> run()
	- validateAndGetActiveCommandLine(commandLine); 	�����Ƿ��� /tmp/.yarn-properties-$user �ļ�����Commaon Cli����; 
	- effectiveConfiguration =getEffectiveConfiguration(commandLine) �����ʹ��� ִ�л���Configuration;
	- executeProgram() -> mainMethod.invoke() ִ�� WordCount��Ӧ��App�� main()����
	- env.execute() ��ʼ�ύJobִ��; 

cli.CliFrontend.main(){
	// ���ض�� ������, Ĭ��3��: GenericCLI, FlinkYarnSessionCli, DefaultCLI;  
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
											// ���,ֻҪ args=>commandLine �в����� "m" ����,���� canApplyYarn��==ture ? Ĭ�϶����� yarn?
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
										// ����/tmp/.yarn-properties-bigdata. ($java.io.tmpdir/.yarn-properties-$user/ Ŀ¼�²鿴 ��� ApplicationID ��Ӧ��session; 
										File yarnPropertiesLocation = getYarnPropertiesLocation(yarnPropertiesFileLocation);{
											if (yarnPropertiesFileLocation != null) {
												propertiesFileLocation = yarnPropertiesFileLocation;
											}else {
												propertiesFileLocation = System.getProperty("java.io.tmpdir");
											}
											return new File(propertiesFileLocation, YARN_PROPERTIES_FILE + currentUser);
										}
										yarnPropertiesFile.load(new FileInputStream(yarnPropertiesLocation));
										
										final String yarnApplicationIdString =yarnPropertiesFile.getProperty(YARN_APPLICATION_ID_KEY);// ��ȡapplicationID
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
					// ������Ч�ĺ�������,���� execution.target, 
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
							// �ѻ��������͸�ִ�������� ��װ�� StreamContextEnvironment
							ContextEnvironment.setAsContext();
							StreamContextEnvironment.setAsContext();
							
							program.invokeInteractiveModeForExecution();{
								mainMethod = entryClass.getMethod("main", String[].class);
								// ִ�� App��main()����,�� WordCount.main()
								mainMethod.invoke(null, (Object) args);{
									// app�ص� env.execute()����������ҵ��ִ��; Դ��� flink-core: ExecutionEnvironment.execute()
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


// 1.1.1  flink-1.9.x �� CliFrontend.run(): flink run����

CliFrontend.main(){
	
	List<CustomCommandLine<?>> customCommandLines = loadCustomCommandLines(Configuration configuration, String configurationDirectory);{
		customCommandLines = new ArrayList<>(2);
		
	}
	
	CliFrontend.run(){
		final CustomCommandLine<?> customCommandLine = getActiveCustomCommandLine(commandLine);
		// ���� ���غ͹��� flink-yarn_xx.jar����: FlinkYarnSessionCli ����,������سɹ�����뵽 commandLines��; 
		try{
			customCommandLines.add(loadCustomCommandLine("org.apache.flink.yarn.cli.FlinkYarnSessionCli",configuration,configurationDirectory,"y","yarn"));{
				// �����쳣̫������, ��� flink-yarn(������flink-dist��)����,�����hadoop+yarn���������, ���� forName()�ɹ�;
				// ��� HADOOP_CLASSPATH����CP��ȱ�� yarn,flink-yarn�����,����ͻ�ʧ��;
				Class<? extends CustomCommandLine> customCliClass =Class.forName(className).asSubclass(CustomCommandLine.class);
				Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);
				return constructor.newInstance(params);
			}
		}catch (NoClassDefFoundError | Exception e) {
			LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
		}
		
		
		runProgram(customCommandLine, commandLine, runOptions, program);{
			
			ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);{
				// ����yarn ģʽ����, 
				FlinkYarnSessionCli.createClusterDescriptor(CommandLine commandLine){
					Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
					return createDescriptor();
				}
				
				// ����Ĭ�ϵ�
				DefaultCLI.createClusterDescriptor(CommandLine commandLine){
					// Ĭ��CmdLine�ǽ��� -m(--jobmanager) �� host:port��ʽ;
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
			
			//Ϊ�յĻ�, clusterDescriptor ������1����Ⱥ;
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
					yarnClient.init(yarnConfiguration);{// YarnClientImpl�ĸ��� AbstractService.init()
						serviceInit(config);// YarnClientImpl.serviceInit()
							timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
								TimelineUtils.buildTimelineTokenService(conf);
									YarnJacksonJaxbJsonProvider.configObjectMapper(mapper): 
										// ����ԭ��
										YarnJacksonJaxbJsonProvider ��ʵ�ֵľ��� javax.ws.rs.ext.MessageBodyReader, λ�ڰ� jersey-core-1.9.jar��;
										jersey-core-1.9.jar ���� jax/lib, $HADOOP_HOME/share/hadoop/xx/lib��, ��flink-shaded-hadoop-2.jar�ְ���,��û��; 
								

/** flik-yarn_2.11_1.9.1 src: 
	FlinkYarnSessionCli.createClusterDescriptor()
		getClusterDescriptor(configuration);//FlinkYarnSessionCli.getClusterDescriptor()
			yarnClient.init(yarnConfiguration);{// YarnClientImpl�ĸ��� AbstractService.init()
				serviceInit(config);// YarnClientImpl.serviceInit()
					timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
						TimelineUtils.buildTimelineTokenService(conf);
							YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
							
								YarnJacksonJaxbJsonProvider ��ʵ�ֵľ��� javax.ws.rs.ext.MessageBodyReader, λ�ڰ� jersey-core-1.9.jar��;
								jersey-core-1.9.jar ���� jax/lib, $HADOOP_HOME/share/hadoop/xx/lib��, ��flink-shaded-hadoop-2.jar�ְ���,��û��; 
								

*/

FlinkYarnSessionCli.createClusterDescriptor(CommandLine commandLine){
	Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
	return createDescriptor();{
		AbstractYarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor(configuration);{//FlinkYarnSessionCli.getClusterDescriptor()
			// YarnClient /YarnClientImpl λ�� hadoop-yarn-client-version.jar��, 
			// flink-shaded-hadoop-2.jar���ְ���Ҳ����;
			YarnClient yarnClient = YarnClient.createYarnClient();{
				YarnClient client = new YarnClientImpl();
				return client;
			}
			//YarnClientImpl extends YarnClient[ extends AbstractService ], 
			yarnClient.init(yarnConfiguration);{// YarnClientImpl�ĸ��� AbstractService.init()
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
YarnClientImpl[����AbstractService].init(yarnConfiguration);{// YarnClientImpl�ĸ��� AbstractService.init()
	setConfig(conf);
	serviceInit(config);{// YarnClientImpl.serviceInit()
		asyncApiPollIntervalMillis =conf.getLong(YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS,YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
		submitPollIntervalMillis = conf.getLong(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS, YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
		
		if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
			timelineServiceEnabled = true;
			timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
			// �����������: NoClassDefFoundError: javax/ws/rs/ext/MessageBodyReader
			timelineService = TimelineUtils.buildTimelineTokenService(conf);{
				static { // ��̬����, ������ YarnJacksonJaxbJsonProvider
					mapper = new ObjectMapper();
					// YarnJacksonJaxbJsonProvider ��ʵ�ֵľ��� javax.ws.rs.ext.MessageBodyReader, λ�ڰ� jersey-core-1.9.jar��;
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





















/** 2	env.execute() �ύִ��
*
*/





// 2.1 env.execute() ������ҵִ��:  env.execute() : ������ӦFactory��Executor,����������, submittJob()�ύִ��; 
env.execute()
	- executorServiceLoader.getExecutorFactory() ͨ�� ���غͱȽ����е� PipelineExecutorFactory.name()�Ƿ�==  execution.target
	- PipelineExecutorFactory.getExecutor() ������Ӧ PipelineExecutorʵ����: YarnSession,YarnPerJob, KubernetesExecutor,LocalExecutor ��; 
	- PipelineExecutor.execute() �ύִ����Ӧ��job��ҵ; 


ExecutionEnvironment.execute(){
	// Streaming ��ִ��
	StreamExecutionEnvironment.execute(){
		return execute(getStreamGraph(jobName));{
			final JobClient jobClient = executeAsync(streamGraph);
			jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));
			return jobExecutionResult;
		}
	}
	// �ֱ���ִ�л����� Զ��ִ�л���
	LocalStreamEnvironment.execute(){
		return super.execute(streamGraph);
	}
	
	RemoteStreamEnvironment.execute(){
		
	}
	StreamContextEnvironment.execute(){};
	StreamPlanEnvironment.execute();{}// ? strema sql ?
	
}




// 2.2 ����������ĵ��� Streamģʽ �첽ִ����ҵ
// PipelineExecutor.execute() clusterClient.submitJob(): RestClient.sendRequest() ��Զ��JobManager���̷��� JobSubmit ����
StreamExecutionEnvironment.executeAsync(StreamGraph streamGraph);{
	// ���ﶨ���� ִ��ģʽ��ִ������; ��Ҫͨ�� ���غͱȽ����е� PipelineExecutorFactory.name()�Ƿ�==  execution.target
	final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);{//core.DefaultExecutorServiceLoader.
		final ServiceLoader<PipelineExecutorFactory> loader = ServiceLoader.load(PipelineExecutorFactory.class);
		while (loader.iterator().hasNext()) {
			// ���� execution.target ������� PipelineExecutorFactory.NAME ���бȽ�,���Ƿ����; 
			boolean isCompatible = factories.next().isCompatibleWith(configuration);{
				RemoteExecutorFactory.isCompatibleWith(){
					return RemoteExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET)); //execution.target==remote
				}
				LocalExecutorFactory.isCompatibleWith(){ // ��execution.target== local
					return LocalExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
				}
				KubernetesSessionClusterExecutorFactory.isCompatibleWith(){//��execution.target�Ƿ�== kubernetes-session
					return configuration.get(DeploymentOptions.TARGET).equalsIgnoreCase(KubernetesSessionClusterExecutor.NAME);
				}
				//Yarn�����ֲ���ģʽ:  yarn-per-job, yarn-session, yarn-application
				YarnSessionClusterExecutorFactory.isCompatibleWith(){ // ��execution.target== yarn-session  
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
		return compatibleFactories.get(0); // ֻ�ܶ���1�� PipelineExecutorFactory, ���򱨴�; 
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
								dispatcherGateway.submitJob(): ����Զ��Rpc����: ʵ��ִ�� Dispatcher.submitJob()
								Dispatcher.submitJob(){ //Զ��Rpc����,�����ؽ��;
									//������������:
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
			
			// YarnCluster, KubeClient �ȶ��� ���
			AbstractSessionClusterExecutor.execute(){
				final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
				// �жϺʹ�����Ҫ�� Cluster���Ӷ�
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
						// ����request: JobSubmitRequestBody
						Tuple2<JobSubmitRequestBody, Collection<FileUpload>> requestFuture= jobGraphFileFuture.thenApply(){
							final JobSubmitRequestBody requestBody =new JobSubmitRequestBody(jobGraphFile.getFileName().toString(),jarFileNames,artifactFileNames);
							return Tuple2.of(requestBody, Collections.unmodifiableCollection(filesToUpload));
						}
						// ����JobSumbit����: sendRequest(request)
						submissionFuture= sendRetriableRequest(request);{//RestClusterClient.
							getWebMonitorBaseUrl()
							return restClient.sendRequest(messageParameters,request, filesToUpload);{//RestClient.
								return sendRequest();{//RestClient.sendRequest()
									String targetUrl = MessageParameters.resolveUrl(versionedHandlerURL, messageParameters);// = /v1/jobs
									objectMapper.writeValue(new StringWriter(), request);
									Request httpRequest =createRequest(targetUrl,payload);
									// ������Ⱥ: bdnode102.hjq.com:36384(YarnSessionClusterEntrypoint) JobManager����JobSubmitRequest
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
	} catch (ExecutionException executionException) {//ִ��ʧ��,�������׳��쳣; 
		throw new FlinkException(String.format("Failed to execute job '%s'.", streamGraph.getJobName()),strippedException);
	}
}



// 2.2.1 JobManagerģ��, ��ӦJobSubmit������߼�: submitJob(): RestClient.sendRequest() -> 






