
/** 1 flinkrun: CliFrontend ����
*/

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



// 1.2 Yarn Cli 
// 1. FlinkYarnSessionCli ����: "main"�߳� ��������yarnRM ���� amContainer 

// Cli��main�߳���Ҫ�Ǵ���YarnClient, �����չ���1�� appContext: ApplicationSubmissionContext ,����Java��������jar/env������; 
// YarnClusterDescriptor.startAppMaster() �д��� appContext,���� yarnClient.submitApplication(appContext) ����Yarn;
// %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%
FlinkYarnSessionCli.run()
	- yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration); ����Yarn RM,������RMClient;
	- yarnClusterDescriptor.deploySessionCluster() 
	- yarnApplication = yarnClient.createApplication(); ����Application
	- startAppMaster(); 
		* appContext = yarnApplication.getApplicationSubmissionContext(); ����Ӧ��ִ�������� appCtx;
		* amContainer =setupApplicationMasterContainer(); �д��� %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% ��ʽ������;
		* yarnClient.submitApplication(appContext); �� appCtx����RM/NM ����Զ��Container/Java��������; 
	

FlinkYarnSessionCli.main(){
	final String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
	final FlinkYarnSessionCli cli = new FlinkYarnSessionCli(flinkConfiguration,configurationDirectory,"");
	retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));{//FlinkYarnSessionCli.run(){
		final CommandLine cmd = parseCommandLineOptions(args, true);
		// ��Ҫ��ʱ1:  ����Yarn ResurceManager
		final YarnClusterDescriptor yarnClusterDescriptor =yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration);{
			return getClusterDescriptor(configuration);{
				final YarnClient yarnClient = YarnClient.createYarnClient();
				yarnClient.init(yarnConfiguration);{
					super.serviceStart();
				}
				yarnClient.start();{//AbstractService.start()
					serviceStart();{//YarnClientImpl.serviceStart()
						rmClient = ClientRMProxy.createRMProxy(getConfig(),ApplicationClientProtocol.class);{
							return createRMProxy(configuration, protocol, INSTANCE);{//RMProxy.createRMProxy()
								RetryPolicy retryPolicy = createRetryPolicy(conf);
								if (HAUtil.isHAEnabled(conf)) {
									RMFailoverProxyProvider<T> provider =instance.createRMFailoverProxyProvider(conf, protocol);
									return (T) RetryProxy.create(protocol, provider, retryPolicy);
								}else{// ��HA, ����; 
									InetSocketAddress rmAddress = instance.getRMAddress(conf, protocol);
									LOG.info("Connecting to ResourceManager at " + rmAddress);
									T proxy = RMProxy.<T>getProxy(conf, protocol, rmAddress);
									return (T) RetryProxy.create(protocol, proxy, retryPolicy);
								}
							}
						}
						if (historyServiceEnabled) {
							historyClient.start();
						}
					}
				}
				return new YarnClusterDescriptor(configuration,yarnConfiguration,yarnClient);
			}
		}
		if (cmd.hasOption(applicationId.getOpt())) {
			clusterClientProvider = yarnClusterDescriptor.retrieve(yarnApplicationId);
		}else{
			final ClusterSpecification clusterSpecification = yarnClusterClientFactory.getClusterSpecification(effectiveConfiguration);
			// ��Ҫ��ʱ2: ����Ӧ��; 
			clusterClientProvider = yarnClusterDescriptor.deploySessionCluster(clusterSpecification);{//YarnClusterDescriptor.deploySessionCluster()
				return deployInternal(clusterSpecification, getYarnSessionClusterEntrypoint());// Դ����� yarn: resourcenanger-src 
			}
		}
	}
}




/**	1.2.1 flinkCli-yarn-SubmitAM: deploySessionCluster()-> startAppMaster()-> yarnClient.submitApplication(appContext) 

YarnClusterDescriptor.startAppMaster(): ���������� am: ApplicationMaster
	//1. ����AM����: YarnClusterDescriptor.setupApplicationMasterContainer(): ����%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% ����AM����;
	//2. ƴ��CLASSPATH: YarnClusterDescriptor.startAppMaster()ƴ��$CLASSPATH,���β���: $FLINK_CLASSPATH() + yarn.application.classpath ,�乹������
			userClassPath(jobGraph.getUserJars(), pipeline.jars, usrlib) 
			* 	systemClassPaths = yarn.ship-files���� + $FLINK_LIB_DIR������jars + logConfigFile;
			*	localResourceDescFlinkJar.getResourceKey() + jobGraphFilename + "flink-conf.yaml"
			yarn.application.classpath Ĭ�ϲ���: $HADOOP_CONF_DIR�� share�µ�common,hdfs,yar3��ģ���Ŀ¼;
	//3. ����yarn api: YarnClientImpl.submitApplication() ��Yarn RMͨ�Ų��ύ���� ApplicationMaster: YarnSessionClusterEntrypoint;
*/

YarnClusterDescriptor.deploySessionCluster(ClusterSpecification clusterSpecification);{//YarnClusterDescriptor.deploySessionCluster()
	return deployInternal(clusterSpecification, getYarnSessionClusterEntrypoint());{
		isReadyForDeployment(clusterSpecification);
		checkYarnQueues(yarnClient);
		final YarnClientApplication yarnApplication = yarnClient.createApplication();
		final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();
		freeClusterMem = getCurrentFreeClusterResources(yarnClient);
		final int yarnMinAllocationMB = yarnConfiguration.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
		ApplicationReport report =startAppMaster();{//YarnClusterDescriptor.startAppMaster()
			final FileSystem fs = FileSystem.get(yarnConfiguration);
			ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
			final List<Path> providedLibDirs =Utils.getQualifiedRemoteSharedPaths(configuration, yarnConfiguration);
			final YarnApplicationFileUploader fileUploader =YarnApplicationFileUploader.from();
			
			userJarFiles.addAll(jobGraph.getUserJars().stream().map(f -> f.toUri()).map(Path::new).collect(Collectors.toSet()));
			userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
			// ����AM(ApplicationMaster)����Դ: amContainer ��Ҫ���� env,javaCammand, localResource����jar����Դ;
			processSpec =JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap();{
				CommonProcessMemorySpec processMemory = PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config);{
					if (options.getRequiredFineGrainedOptions().stream().allMatch(config::contains)) {
						
					}else if (config.contains(options.getTotalFlinkMemoryOption())) {//jobmanager.memory.flink.size
						return deriveProcessSpecWithTotalFlinkMemory(config);
					}else if (config.contains(options.getTotalProcessMemoryOption())) {// jobmanager.memory.process.size
						return deriveProcessSpecWithTotalProcessMemory(config);{
							MemorySize totalProcessMemorySize =getMemorySizeFromConfig(config, options.getTotalProcessMemoryOption());
							// MetaspaceĬ�� 256Mb, jobmanager.memory.jvm-metaspace.size
							JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =deriveJvmMetaspaceAndOverheadWithTotalProcessMemory(config, totalProcessMemorySize);
							// Լ���� total - metaspace - overhead = 1024 - 256 -196 = 576Mb
							MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize());
							// �ְ� 576 ��һ���ֲ�heap /offHeap, ���� 448, ���� 128Mb; 
							FM flinkInternalMemory =flinkMemoryUtils.deriveFromTotalFlinkMemory(config, totalFlinkMemorySize);
							return new CommonProcessMemorySpec<>(flinkInternalMemory, jvmMetaspaceAndOverhead);
						}
					}
					return failBecauseRequiredOptionsNotConfigured();
				}
				return new JobManagerProcessSpec(processMemory.getFlinkMemory(), processMemory.getJvmMetaspaceAndOverhead());
			}
			flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
			ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
			
			// ƴ�� %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% ����;
			JobManagerProcessSpec processSpec =JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
			final ContainerLaunchContext amContainer =setupApplicationMasterContainer(yarnClusterEntrypoint, hasKrb5, processSpec);{//YarnClusterDescriptor.
				String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
				javaOpts += " " + flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
				startCommandValues.put("java", "$JAVA_HOME/bin/java");
				startCommandValues.put("jvmmem", jvmHeapMem);{
					jvmArgStr.append("-Xmx").append(processSpec.getJvmHeapMemorySize().getBytes());
					jvmArgStr.append(" -Xms").append(processSpec.getJvmHeapMemorySize().getBytes());
					if (enableDirectMemoryLimit) {//jobmanager.memory.enable-jvm-direct-memory-limit
						jvmArgStr.append(" -XX:MaxDirectMemorySize=").append(processSpec.getJvmDirectMemorySize().getBytes());
					}
					jvmArgStr.append(" -XX:MaxMetaspaceSize=").append(processSpec.getJvmMetaspaceSize().getBytes());
				}
				startCommandValues.put("jvmopts", javaOpts);
				startCommandValues.put("class", yarnClusterEntrypoint);
				startCommandValues.put("args", dynamicParameterListStr);
				//ʹ��yarn.container-start-command-template,���߲���Ĭ�� %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%
				 String commandTemplate =flinkConfiguration.getString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
				String amCommand =BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
			}
			amContainer.setLocalResources(fileUploader.getRegisteredLocalResources());
			// ����env: _FLINK_CLASSPATH ��������
			userJarFiles.addAll(jobGraph.getUserJars().stream().map(f -> f.toUri())); //��� jobGraph.getUserJars() �е�jars
			userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet())); // ��� pipeline.jars�е�jars;
			final List<String> userClassPaths =fileUploader.registerMultipleLocalResources(
				userJarFiles, // =  jobGraph.getUserJars() + pipeline.jars 
				userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR, LocalResourceType.FILE); // ���usrlib/Ŀ¼��
			
			//FLINK_CLASSPATH 1: include-user-jar=firstʱ,�� jobGraph.getUserJars() &pipeline.jars &usrlib Ŀ¼��jars �ӵ�ǰ��;
			if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST) classPathBuilder.append(userClassPath).append(File.pathSeparator);//yarn.per-job-cluster.include-user-jar
			// FLINK_CLASSPATH 2: systemClassPaths= shipFiles(yarn.ship-files����) + logConfigFile +systemShipFiles(Sys.FLINK_LIB_DIR����) , ���� localResources���ϴ���13��flink��lib��jar��;
			addLibFoldersToShipFiles(systemShipFiles);{
				String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);//��ϵͳ������ȡFLINK_LIB_DIR ��ֵ;
				effectiveShipFiles.add(new File(libDir));
			}
			for (String classPath : systemClassPaths) classPathBuilder.append(classPath).append(File.pathSeparator);
			// FLINK_CLASSPATH 3: 
			classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
			classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
			classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
			//FLINK_CLASSPATH 6: include-user-jar=lastʱ, ��userClassPath ��jars�ӵ�CP����; 
			if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) classPathBuilder.append(userClassPath).append(File.pathSeparator);
			
			appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());
			appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES,fileUploader.getApplicationDir().toUri().toString());
			// ���� CLASSPATH�Ĳ���
			Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);{
				// 1. �Ȱ� _FLINK_CLASSPATH�� lib��13��flink���jar���ӵ�CP
				addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
				// 2. yarn.application.classpath + 
				String[] applicationClassPathEntries =conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);{
					String valueString = get(name);// ��ȡyarn.application.classpath ����
					if (valueString == null) {// ����YarnĬ��CP: YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH, ����7��;
						return defaultValue;// Ĭ��YarnCP����4��: CONF_DIR�� share�µ�common,hdfs,yar3��ģ���Ŀ¼;
					} else {
						return StringUtils.getStrings(valueString);
					}
				}
				for (String c : applicationClassPathEntries) {
					addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
				}
			}
			amContainer.setEnvironment(appMasterEnv);
			appContext.setAMContainerSpec(amContainer);
			
			// ����CPU/Memory��Դ��С; 
			capability.setMemory(clusterSpecification.getMasterMemoryMB());
			capability.setVirtualCores(flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES));
			appContext.setResource(capability);
			
			setApplicationTags(appContext);
			yarnClient.submitApplication(appContext);{//YarnClientImpl.submitApplication()
				SubmitApplicationRequest request =Records.newRecord(SubmitApplicationRequest.class);
				request.setApplicationSubmissionContext(appContext);
				rmClient.submitApplication(request);{
					// yarn ��resourceManager�� resourcemanager.ClientRMService ���д���
					
				}
				while (true) {// ��waitingStates ���������� applicationId
					if (!waitingStates.contains(state)) {
						LOG.info("Submitted application " + applicationId);
						break;
					}
				}
				return applicationId;
			}
			
			LOG.info("Waiting for the cluster to be allocated");
			while (true) {
				report = yarnClient.getApplicationReport(appId);
				YarnApplicationState appState = report.getYarnApplicationState();
				switch (appState) {
					case FAILED: case KILLED:
						throw new YarnDeploymentException();
					case RUNNING:case FINISHED:
						break loop;
					default:
				}
				Thread.sleep(250);
			}
			
		}
		return () -> {return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());};
		
	}
}


// flink yarn CLASSPATH ����
{
	
		
	/* ���� 
		jobGraph.userJars:		pipeline.jars:			���� args[0] or -j or --jarfile ����ָ��
		jobGraph.classpaths:	pipeline.classpaths:	���� -C or --classpath ����ָ��
	*/
	CliFrontend.run(){
		final List<URL> jobJars = getJobJarAndDependencies(programOptions);{
			// entryPointClass ���� -c/--class����ָ��ֵ; 
			String entryPointClass = programOptions.getEntryPointClassName(); //δָ��δ��;
			// jarFilePath ��App Jar��, Ĭ�� args[0] �� -j �� --jarfile ָ��;
			String jarFilePath = programOptions.getJarFilePath();
			File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;
			return PackagedProgram.getJobJarAndDependencies(jarFile, entryPointClass);{
				URL jarFileUrl = loadJarFile(jarFile);
			}
		}
		Configuration effectiveConfiguration =getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);{
			ExecutionConfigAccessor executionParameters = ExecutionConfigAccessor.fromProgramOptions(checkNotNull(programOptions), checkNotNull(jobJars));{
					final Configuration configuration = new Configuration();
					// ��config��д�� execution.attached,pipeline.classpaths��4������;
					options.applyToConfiguration(configuration);{
						// ���� --classpath or -C ������ָ��;
						ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, getClasspaths(), URL::toString);
					}
					ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, jobJars, Object::toString);
					return new ExecutionConfigAccessor(configuration);
				}
		}
	}
	
	AbstractJobClusterExecutor.execute(){
		final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);{
			ExecutionConfigAccessor executionConfigAccessor =ExecutionConfigAccessor.fromConfiguration(configuration);{
				return new ExecutionConfigAccessor(checkNotNull(configuration));
			}
			JobGraph jobGraph =FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());
			// ��config�ж�ȡ pipeline.jars ��Ϊ����;
			List<URL> jarFilesToAttach = executionConfigAccessor.getJars();{
				return ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URL::new);//ȡpipeline.jars����;
			}
			jobGraph.addJars(jarFilesToAttach);{
				for (URL jar : jarFilesToAttach) {
					addJar(new Path(jar.toURI()));{//�� jobGraph.userJars:List<Path> ����� pipeline.jars��������ЧURI
						if (!userJars.contains(jar)) {
							userJars.add(jar);
						}
					}
				}
			}
			//��ȡpipeline.classpaths �ı���ֵ,����ֵ jobGraph.classpaths: List<URL> 
			jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
		}
		clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);
	}
	
	/* 1. ���� dist,ship,archives��Դ·��: 
		flinkJarPath:	�� yarn.flink-dist-jar����,���߽� this.codesource������Ϊ dist��·��; 	δ����Ĭ��: /opt/flink/flink-1.12.2/lib/flink-dist_2.11-1.12.2.jar
		shipFiles:		�� yarn.ship-files��ȡ			δ��Ĭ��Ϊ��;
		shipArchives:	�� yarn.ship-archives ��ȡ 		δ��Ĭ��Ϊ��;
	*/
	AbstractJobClusterExecutor.execute().createClusterDescriptor().getClusterDescriptor(){
		final YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration); yarnClient.start();
		
		new YarnClusterDescriptor();{
			this.userJarInclusion = getUserJarInclusionMode(flinkConfiguration);
			//1 �� yarn.flink-dist-jar����,���߽� this.codesource������Ϊ dist��·��,����ֵ YarnClusterDescriptor.flinkJarPath ����;
			getLocalFlinkDistPath(flinkConfiguration).ifPresent(this::setLocalJarPath);{
				String localJarPath = configuration.getString(YarnConfigOptions.FLINK_DIST_JAR); // yarn.flink-dist-jar
				if (localJarPath != null) {
					return Optional.of(new Path(localJarPath));
				}
				final String decodedPath = getDecodedJarPath();{//�� Class.pd.codesource.location.path //this������� flink-dist.jar������;
					final String encodedJarPath =getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
					return URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
				}
				return decodedPath.endsWith(".jar")? Optional.of(new Path(new File(decodedPath).toURI())): Optional.empty();
			}
			//2 �� yarn.ship-files��ȡ ��Դ�ļ�·��,����ֵ YarnClusterDescriptor.shipFiles ����;
			decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_FILES){// YarnClusterDescriptor.decodeFilesToShipToCluster
				final List<File> files =ConfigUtils.decodeListFromConfig(configuration, configOption, File::new);// yarn.ship-files ���� ship:jar����?
				return files.isEmpty() ? Optional.empty() : Optional.of(files);
			}.ifPresent(this::addShipFiles);
			//3 �� yarn.ship-archives ��ȡ ��Դ�ļ�·��,����ֵ YarnClusterDescriptor.shipArchives ����;
			decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_ARCHIVES).ifPresent(this::addShipArchives);
			this.yarnQueue = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_QUEUE);
		}
	}
	
	
	

	/* 2. ����$FLINK_CLASSPATH, CLASSPATH
		List<Path> providedLibDirs:		�� yarn.provided.lib.dirs �ж�ȡ����	δ������δ��;
		
	$FLINK_CLASSPATH 
		systemClassPaths = yarn.ship-files���� + $FLINK_LIB_DIR������jars + logConfigFile
			- fileUploader.providedSharedLibs �е� ��dist��plugin ����;
				* List<Path> providedLibDirs: yarn.provided.lib.dirs��������Чdir����
			- uploadedDependencies: �����systemShipFiles�� ��PUBLIc&& ��dist �Ĳ���;
				- systemShipFiles
					- logConfigFilePath
					- $FLINK_LIB_DIR, �� providedLibDirs(yarn.provided.lib.dirs) Ϊ��ʱ, ����� $FLINK_LIB_DIR
			- userClassPaths:  ����yarn.per-job-cluster.include-user-jar=orderʱ, ��� userJarFiles
				- userJarFiles:	
					* JobGraph.userJars 	pipeline.jars(args[0]/-j/--jarfileָ��) ,��examples/batch/WordCount.jar
					* jarUrls:				pipeline.jars:		�����ҵ� YarnApplicationģʽʱ, �ű��ӵ� userJarFiles ��, һ���� PerJob/YarnSession ���Բ����� CP;


		userClassPath: 		ȡ[��PUBLIc &&��dist]��userJarFiles;
			- userJarFiles:	 
				- JobGraph.userJars 	pipeline.jars(args[0]/-j/--jarfileָ��) ,��examples/batch/WordCount.jar
				- pipeline.jars:		�����ҵ� YarnApplicationģʽʱ, �ű��ӵ� userJarFiles ��, һ���� PerJob/YarnSession ���Բ����� CP;
				
		
		flinkJarPath: (yarn.flink-dist-jar �� this.codesource.localpath
			yarn.flink-dist-jar
				��������,��ʹ�� this.codesource.localpath(��flink-dist����)
		
		localResourceDescFlinkJar.getResourceKey()
		jobGraphFilename
		"flink-conf.yaml"
		
	yarn.application.classpath
		$HADOOP_CONF_DIR
		common: $HADOOP_COMMON_HOME/share/*/common/*
		hdfs: $HADOOP_HDFS_HOME/share/*/hdfs/*
		yarn: $HADOOP_YARN_HOME/share/*/yarn/*
		
	*/

	YarnClusterDescriptor.startAppMaster(){//YarnClusterDescriptor.startAppMaster()
		final FileSystem fs = FileSystem.get(yarnConfiguration);
		ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
		// �� yarn.provided.lib.dirs �ж�ȡ����;��������ӵ� systemClassPaths->CLASSPATH; ��������,������ü���$FLINK_LIB_DIR��systemClassPaths(->CP);
		final List<Path> providedLibDirs =Utils.getQualifiedRemoteSharedPaths(configuration, yarnConfiguration);{
			return getRemoteSharedPaths(){//Utils.
				// yarn.provided.lib.dirs
				final List<Path> providedLibDirs =ConfigUtils.decodeListFromConfig(configuration, YarnConfigOptions.PROVIDED_LIB_DIRS, strToPathMapper);
				return providedLibDirs;
			}
		}
		//�ص��ǹ��� providedLibDirs(yarn.provided.lib.dirs) �� ΪdirĿ¼��,����ֵ�� fileUploader.providedSharedLibs
		final YarnApplicationFileUploader fileUploader =YarnApplicationFileUploader.from(fs,providedLibDirs);{new YarnApplicationFileUploader(){
			this.applicationDir = getApplicationDir(applicationId);
			this.providedSharedLibs = getAllFilesInProvidedLibDirs(providedLibDirs);{
				Map<String, FileStatus> allFiles = new HashMap<>();
				providedLibDirs.forEach(path -> {
					if (!fileSystem.exists(path) || !fileSystem.isDirectory(path)) {
						LOG.warn("Provided lib dir {} does not exist or is not a directory. Ignoring.",path);
					}else{
						final RemoteIterator<LocatedFileStatus> iterable =fileSystem.listFiles(path, true).forEach(()-> allFiles.put(name, locatedFileStatus););
					}
				});
				return Collections.unmodifiableMap(allFiles);
			}
		}}
		
		// ��shipFiles��(yarn.ship-files ���), ��ӵ� systemShipFiles��,������ -> uploadedDependencies -> systemClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		Set<File> systemShipFiles = new HashSet<>(shipFiles.size());
		for (File file : shipFiles) {
			 systemShipFiles.add(file.getAbsoluteFile());
		}
		// $internal.yarn.log-config-file ,���������ӵ� systemShipFiles��; һ��������: /opt/flink/conf/log4j.properties; ���� -> uploadedDependencies -> systemClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		final String logConfigFilePath =configuration.getString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
		if (null !=logConfigFilePath) {
			systemShipFiles.add(new File(logConfigFilePath));
		}
		// �� yarn.provided.lib.dirs������, ����� FLINK_LIB_DIR �� systemShipFiles -> systemClassPaths -> CLASSPATH;
		if (providedLibDirs == null || providedLibDirs.isEmpty()) {
			addLibFoldersToShipFiles(systemShipFiles);{//YarnClusterDescriptor.addLibFoldersToShipFiles()
				String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);// �� $FLINK_LIB_DIR ��������;
				if (libDir != null) {
					File directoryFile = new File(libDir);
					if (directoryFile.isDirectory()) {
						effectiveShipFiles.add(directoryFile);//effectiveShipFiles ������� systemShipFiles;
					}
				}else if(shipFiles.isEmpty()){// ��� null == libDir($FLINK_LIB_DIRΪ��),�� shipFiles(yarn.ship-files)ҲΪ��,��warn�澯;
					LOG.warn("Environment variable 'FLINK_LIB_DIR' not set and ship files have not been provided manually. Not shipping any library files");
				}
			}
		}
		
		final Set<Path> userJarFiles = new HashSet<>();
		// ��JobGraph.userJars��ӵ� userJarFiles��; 	Ӧ�þ��� -jarָ����App��,��examples/batch/WordCount.jar;  ������ userJarFiles-> userClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		if (jobGraph != null) {
			List<Path> jobUserJars = jobGraph.getUserJars().stream().map(f -> f.toUri()).map(Path::new).collect(Collectors.toSet());
			userJarFiles.addAll(jobUserJars);
		}
		//�� pipeline.jars�ж�ȡֵ����ֵ�� jarUrls;  Ĭ�Ͼ���-jar ·��: examples/batch/WordCount.jar
		final List<URI> jarUrls =ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URI::create);// ��pipeline.jars��ȡֵ;
		//ֻ�е� YarnApplication ģʽʱ,�Ż�ӵ� userClassPaths ->$FLINK_CLASSPATH��;  һ�� yarnClusterEntrypoint�� YarnJob or YarnSession, ���Բ����� CP;
		if (jarUrls != null && YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)) {
			userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
		}
		
		// Register all files in provided lib dirs as local resources with public visibility and upload the remaining dependencies as local resources with APPLICATION visibility.
		// ��fileUploader.providedSharedLibs( yarn.provided.lib.dirs��������Чdir����) �е� ��dist��plugin��, 
		final List<String> systemClassPaths = fileUploader.registerProvidedLocalResources();{// YarnApplicationFileUploader.registerProvidedLocalResources()
			final ArrayList<String> classPaths = new ArrayList<>();
			providedSharedLibs.forEach((fileName, fileStatus)->{
				final Path filePath = fileStatus.getPath();
				if (!isFlinkDistJar(filePath.getName()) && !isPlugin(filePath)) {// �ѷ�dist��plugin�������ļ�,��ӵ� classPaths��;
					classPaths.add(fileName);
				}else if (isFlinkDistJar(filePath.getName())) { // �����flink-dist�ļ�,ֱ�Ӹ�ֵ�� flinkDist;
					flinkDist = descriptor;
				}
			});
		}
		// ��systemShipFiles��(logConfigFile + $FLINK_LIB_DIR(������yarn.provided.lib.dirsʱ) )���ݸ��� shipFiles;
		Collection<Path> shipFiles = systemShipFiles.stream().map(e -> new Path(e.toURI())).collect(Collectors.toSet());
		// ��shipFiles��(1��2��)����(�ݹ����)����,���˳� [PUBLIC] && ��dist] ������ archives & resources, һ�𷵻ظ���uploadedDependencies;
		final List<String> uploadedDependencies =fileUploader.registerMultipleLocalResources(shipFiles,Path.CUR_DIR,LocalResourceType.FILE);{
			// ����shipFiles��,����Ŀ¼�����е��ļ�,�����ص� localPath��;
			final List<Path> localPaths = new ArrayList<>();
			for (Path shipFile : shipFiles) {
				if (Utils.isRemotePath(shipFile.toString())) {
					
				}else{
					final File file = new File(shipFile.toUri().getPath());
					if (file.isDirectory()) {// 
						Files.walkFileTree();//��Ŀǰ���������ö�����?
					}
				}
				localPaths.add(shipFile);
				relativePaths.add(new Path(localResourcesDirectory, shipFile.getName()));
			}
			// ֻ�� ��dist( !isFlinkDistJar ) �� ��Publich ( !alreadyRegisteredAsLocalResource() ) ��filePath�Żᱻ���;
			for (int i = 0; i < localPaths.size(); i++) {
				if (!isFlinkDistJar(relativePath.getName())) {
					// ��YarnApplicationFileUploader �� remotePath,envShipResourceList,localResources�ȱ��� ���;
					YarnLocalResourceDescriptor resourceDescriptor =registerSingleLocalResource(key,loalpath,true,true);
					if (!resourceDescriptor.alreadyRegisteredAsLocalResource(){// ֻ�з�PUBLIC�����������Դ �����; log4j.properties��Ϊ��APP���𱻹��˵�;
						return this.visibility.equals(LocalResourceVisibility.PUBLIC)
					}) {
						if (key.endsWith("jar")) { //��jar���㵽 archives,
							archives.add(relativePath.toString());
						}else{ //���з�jar��file ���㵽 resource��; 
							resources.add(relativePath.getParent().toString());
						}
					}
				}
			}
			final ArrayList<String> classPaths = new ArrayList<>();
			resources.stream().sorted().forEach(classPaths::add);
			archives.stream().sorted().forEach(classPaths::add);
			return classPaths;
		}
		systemClassPaths.addAll(uploadedDependencies);
		
		if (providedLibDirs == null || providedLibDirs.isEmpty()) {
			// ȡ$FLINK_PLUGINS_DIR����ֵ,�����Ĭ��"plugins" ��Ϊ Ϊ���Ŀ¼����ӵ�shipOnlyFiles��;
			Set<File> shipOnlyFiles = new HashSet<>();
			addPluginsFoldersToShipFiles(shipOnlyFiles);{//YarnApplicationFileUploader.addPluginsFoldersToShipFiles()
				Optional<File> pluginsDir = PluginConfig.getPluginsDir();{
					String pluginsDir =System.getenv().getOrDefault(ConfigConstants.ENV_FLINK_PLUGINS_DIR,//FLINK_PLUGINS_DIR
						ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS);// "plugins"
					File pluginsDirFile = new File(pluginsDir);
					if (!pluginsDirFile.isDirectory()) {
						return Optional.empty();
					}
					return Optional.of(pluginsDirFile);
				}
				pluginsDir.ifPresent(effectiveShipFiles::add);// ���plugins���ô���,�� add (��shipOnlyFiles),������;
			}
			fileUploader.registerMultipleLocalResources(); // 
		}
		if (!shipArchives.isEmpty()) {//��yarn.ship-archives��Ϊ��,
			shipArchivesFile = shipArchives.stream().map(e -> new Path(e.toURI())).collect(Collectors.toSet());
			fileUploader.registerMultipleLocalResources(shipArchivesFile);
		}
		
		// localResourcesDir= "."
		String localResourcesDir= userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR, LocalResourceType.FILE;
		final List<String> userClassPaths =fileUploader.registerMultipleLocalResources(userJarFiles, localResourcesDir);{// ��������[��PUBLIC] && ��dist] 
			for (int i = 0; i < localPaths.size(); i++) {
				final Path relativePath = localPaths.get(i).get(i);
				if (!isFlinkDistJar(relativePath.getName())) {
					// ֻҪ����PUBLIC �����, �����; ����� userJar(��:examples/batch/WordCount.jar) ���ɹ����;
					if (!resourceDescriptor.alreadyRegisteredAsLocalResource(){// ֻҪ��PUBLIC���������, �����; 
						return this.visibility.equals(LocalResourceVisibility.PUBLIC)
					}) {
						if (key.endsWith("jar")) { //��jar���㵽 archives,
							archives.add(relativePath.toString());
						}else{ //���з�jar��file ���㵽 resource��; 
							resources.add(relativePath.getParent().toString());
						}
					}
				}
			}
		}
		// ��yarn.per-job-cluster.include-user-jar=orderʱ, ���userClassPaths�� systemClassPath
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.ORDER) {//yarn.per-job-cluster.include-user-jar=orderʱ 
			systemClassPaths.addAll(userClassPaths);
		}
		
		//FLINK_CLASSPATH 1: include-user-jar=firstʱ,�� jobGraph.getUserJars() &pipeline.jars &usrlib Ŀ¼��jars �ӵ�ǰ��;
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST){////yarn.per-job-cluster.include-user-jar=firstʱ, userClassPath��ǰ��;
			classPathBuilder.append(userClassPath).append(File.pathSeparator);
		} 
		Collections.sort(systemClassPaths);
		Collections.sort(userClassPaths);
		StringBuilder classPathBuilder = new StringBuilder();
		
		for (String classPath : systemClassPaths) {// ���system�����CP
            classPathBuilder.append(classPath).append(File.pathSeparator);
        }
		// ��װ flinkJarPath(yarn.flink-dist-jar �� this.codesource.localpath����,��flink-dist��); ����ӵ� classPath��;
		final YarnLocalResourceDescriptor localResourceDescFlinkJar =fileUploader.uploadFlinkDist(flinkJarPath);
		classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
		
		// ��jobGraph���кų��ļ�,���� "job.graph" ��ӵ�classpath;
		if (jobGraph != null) {
			File tmpJobGraphFile = File.createTempFile(appId.toString(), null);
			// ��jobGraph����д������ʱ�ļ�: /tmp/application_1639998011452_00604014191052203287620.tmp
			try (FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
				 ObjectOutputStream obOutput = new ObjectOutputStream(output)) {
                    obOutput.writeObject(jobGraph);
            }
			final String jobGraphFilename = "job.graph";
			configuration.setString(JOB_GRAPH_FILE_PATH, jobGraphFilename);
			fileUploader.registerSingleLocalResource();
			classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
		}
		// ��"flink-conf.yaml" ��ӵ�classPath��;
		File tmpConfigurationFile = File.createTempFile(appId + "-flink-conf.yaml", null);
		BootstrapTools.writeConfiguration(configuration, tmpConfigurationFile);
		String flinkConfigKey = "flink-conf.yaml";
		fileUploader.registerSingleLocalResource(flinkConfigKey);
		classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
		
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) {
            for (String userClassPath : userClassPaths) {
                classPathBuilder.append(userClassPath).append(File.pathSeparator);
            }
        }
		
		
		
		
		
		// FLINK_CLASSPATH 2: systemClassPaths= shipFiles(yarn.ship-files����) + logConfigFile +systemShipFiles(Sys.FLINK_LIB_DIR����) , ���� localResources���ϴ���13��flink��lib��jar��;
		addLibFoldersToShipFiles(systemShipFiles);{
			String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);//��ϵͳ������ȡFLINK_LIB_DIR ��ֵ;
			effectiveShipFiles.add(new File(libDir));
		}
		for (String classPath : systemClassPaths) classPathBuilder.append(classPath).append(File.pathSeparator);
		// FLINK_CLASSPATH 3: 
		classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
		classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
		classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
		//FLINK_CLASSPATH 6: include-user-jar=lastʱ, ��userClassPath ��jars�ӵ�CP����; 
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) classPathBuilder.append(userClassPath).append(File.pathSeparator);
		
		appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());
		appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES,fileUploader.getApplicationDir().toUri().toString());
		// ���� CLASSPATH�Ĳ���
		Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);{
			// 1. �Ȱ� _FLINK_CLASSPATH�� lib��13��flink���jar���ӵ�CP
			addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
			// 2. yarn.application.classpath + 
			String[] applicationClassPathEntries =conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);{
				String valueString = get(name);// ��ȡyarn.application.classpath ����
				if (valueString == null) {// ����YarnĬ��CP: YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH, ����7��;
					return defaultValue;// Ĭ��YarnCP����4��: CONF_DIR�� share�µ�common,hdfs,yar3��ģ���Ŀ¼;
				} else {
					return StringUtils.getStrings(valueString);
				}
			}
			for (String c : applicationClassPathEntries) {
				addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
			}
		}
		amContainer.setEnvironment(appMasterEnv);
		
	}



	
}





/** 1.2.2 yarnCli: submitApplication ��Yarn RMͨ��,�ύ����AM; 
	//1. ����Rpc����: ProtobufRpcEngine.invoke()
	//2. ͨ�ŵȴ��� waitingStates�ͽ�������,���� applicationId
*/

YarnClientImpl.submitApplication(ApplicationSubmissionContext appContext){//YarnClientImpl.submitApplication()
	SubmitApplicationRequest request =Records.newRecord(SubmitApplicationRequest.class);
	request.setApplicationSubmissionContext(appContext);
	
	rmClient.submitApplication(request);{// ApplicationClientProtocolPBClientImpl.submitApplication()
		// yarn ��resourceManager�� resourcemanager.ClientRMService ���д���
		SubmitApplicationRequestProto requestProto= ((SubmitApplicationRequestPBImpl) request).getProto();
		SubmitApplicationResponseProto proto= proxy.submitApplication(null,requestProto){
			// ʵ��ִ��: 
			ProtobufRpcEngine.invoke(Object proxy, Method method, Object[] args){}{
				// method= ApplicationClientProtocol.BlokingInterface.submitApplication()
				RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
				RpcRequestWrapper rpcRequest= new RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId,fallbackToSimpleAuth);
				RpcResponseWrapper val=(RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,rpcRequest);
				Message returnMessage = prototype.newBuilderForType().mergeFrom(val.theResponseRead).build();
				return returnMessage;
			}
		}
		return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null,requestProto));
	}
	while (true) {// ��waitingStates ���������� applicationId
		if (!waitingStates.contains(state)) {
			LOG.info("Submitted application " + applicationId);
			break;
		}
	}
	return applicationId;
}



// 1.3 K8s Cli
// KubernetesSessionCli.main() ��Java�ύ����
org.apache.flink.kubernetes.cli.KubernetesSessionCli.main(){
	final Configuration configuration = getEffectiveConfiguration(args);{
		final CommandLine commandLine = cli.parseCommandLineOptions(args, true);
		final Configuration effectiveConfiguration = new Configuration(baseConfiguration);
        effectiveConfiguration.addAll(cli.toConfiguration(commandLine));
        effectiveConfiguration.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);
        return effectiveConfiguration;
	}
	final ClusterClientFactory<String> kubernetesClusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(configuration);
	final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, configDir);
	int retCode;
	try {
		final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, configDir);
		retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));{//KubernetesSessionCli.run()
			final Configuration configuration = getEffectiveConfiguration(args);
			final ClusterClientFactory<String> kubernetesClusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(configuration);
			String clusterId = kubernetesClusterClientFactory.getClusterId(configuration);
			final FlinkKubeClient kubeClient = FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "client");
			
			if (clusterId != null && kubeClient.getRestService(clusterId).isPresent()) {
				clusterClient = kubernetesClusterDescriptor.retrieve(clusterId).getClusterClient();
			}else{// ��һ��,��������; 
				clusterClient =kubernetesClusterDescriptor
										.deploySessionCluster(kubernetesClusterClientFactory.getClusterSpecification(configuration)){//KubernetesClusterDescriptor.deploySessionCluster
											KubernetesClusterDescriptor.deploySessionCluster(){
												final ClusterClientProvider<String> clusterClientProvider = deployClusterInternal(KubernetesSessionClusterEntrypoint.class.getName(),clusterSpecification, false);
												try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
													LOG.info("Create flink session cluster {} successfully, JobManager Web Interface: {}", clusterId, clusterClient.getWebInterfaceURL());
												}
												return clusterClientProvider;
											}
										}
										.getClusterClient();
				clusterId = clusterClient.getClusterId();
			}
			
			clusterClient.close();
			kubeClient.close();
		}
	} catch (CliArgsException e) {
		retCode = AbstractCustomCommandLine.handleCliArgsException(e, LOG);
	} catch (Exception e) {
		retCode = AbstractCustomCommandLine.handleError(e, LOG);
	}
	System.exit(retCode);
	
	
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






/** 3	FlinkSqlCli 
*
*/



// SqlClient������ TableEnvInit��ʼ���� CatalogManager����;
// client.start().openSession().build(): ExecutionContext.initializeTableEnvironment()��ʼ��Table������Դ, initializeCatalogs()������������Catalogs��curdb;
// A. client.start().open().parseCommand(line).sqlParser.parse(stmt): PlannerContext.createCatalogReader() ��CatalogManager��curCatalog/DB��ΪdefaultSchemas ��װ��FlinkCatalogReader;
// B. client.start().open().callCommand().callSelect(cmdCall):executor.executeQuery():tableEnv.sqlQuery(selectQuery) �ύTable��ѯ����: TableEnvironmentImpl.sqlQuery()

SqlClient.main(){
	final SqlClient client = new SqlClient(true, options);
	client.start();{
		final Executor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
        executor.start();
		final Environment sessionEnv = readSessionEnvironment(options.getEnvironment());
        appendPythonConfig(sessionEnv, options.getPythonConfiguration());
		context = new SessionContext(options.getSessionId(), sessionEnv);
		// ���� ModuleManager, CatalogManager, FunctionCatalog
		String sessionId = executor.openSession(context);{// LocalExecutor.
			String sessionId = sessionContext.getSessionId();// defaul;
			this.contextMap.put(sessionId, createExecutionContextBuilder(sessionContext).build());{//ExecutionContext$Builder.build()
				return new ExecutionContext<>(this.sessionContext,this.sessionState,this.dependencies,,,);{//ExecutionContext()���캯��, ����һ�ѵ�ִ�л���;
					classLoader = ClientUtils.buildUserCodeClassLoader();
					// ��Ҫ�Ļ������������� ���ж�������
					initializeTableEnvironment(sessionState);{//ExecutionContext.initializeTableEnvironment()
						EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
						final TableConfig config = createTableConfig();
						if (sessionState == null) {
							// Step.1 Create environments
							final ModuleManager moduleManager = new ModuleManager();
							final CatalogManager catalogManager =CatalogManager.newBuilder()
										.classLoader(classLoader).config(config.getConfiguration())
										.defaultCatalog(settings.getBuiltInCatalogName(),
												new GenericInMemoryCatalog(settings.getBuiltInCatalogName(),settings.getBuiltInDatabaseName()))
										.build();{//CatalogManager.Builder.build()
											// default_catalog, default_database
											return new CatalogManager(defaultCatalogName,defaultCatalog,new DataTypeFactoryImpl(classLoader, config, executionConfig));
							}
							CommandLine commandLine =createCommandLine(environment.getDeployment(), commandLineOptions);
							clusterClientFactory = serviceLoader.getClusterClientFactory(flinkConfig);
							// Step 1.2 Initialize the FunctionCatalog if required.
							FunctionCatalog functionCatalog =new FunctionCatalog(config, catalogManager, moduleManager);
							// Step 1.3 Set up session state.
							this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);
							// Must initialize the table environment before actually the
							createTableEnvironment(settings, config, catalogManager, moduleManager, functionCatalog);
							// Step.2 Create modules and load them into the TableEnvironment.
							environment.getModules().forEach((name, entry) -> modules.put(name, createModule(entry.asMap(), classLoader)));
							// Step.3 create user-defined functions and temporal tables then register them.
							registerFunctions();
							// Step.4 Create catalogs and register them. ����config�����ļ�,�������Catalog�� curCatalog,curDatabase;
							initializeCatalogs();{// ExecutionContext.initializeCatalogs
								// Step.1 Create catalogs and register them.
								environment.getCatalogs().forEach((name, entry) -> {
												Catalog catalog=createCatalog(name, entry.asMap(), classLoader);
												tableEnv.registerCatalog(name, catalog);{//TableEnvironmentImpl.registerCatalog
													catalogManager.registerCatalog(catalogName, catalog);{//CatalogManager.registerCatalog
														catalog.open();{// ��ͬ��catalog,��ͬ��ʵ��;
															HiveCatalog.open();
														}
														catalogs.put(catalogName, catalog);
													}
												}
											});
								// Step.2 create table sources & sinks, and register them.
								environment.getTables().forEach((name, entry) -> {
												if (entry instanceof SourceTableEntry|| entry instanceof SourceSinkTableEntry) {
													tableSources.put(name, createTableSource(name, entry.asMap()));
												}
												if (entry instanceof SinkTableEntry|| entry instanceof SourceSinkTableEntry) {
													tableSinks.put(name, createTableSink(name, entry.asMap()));
												}
											});
								tableSources.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSourceInternal);
								tableSinks.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSinkInternal);
								// Step.4 Register temporal tables.
								environment.getTables().forEach((name, entry) -> {registerTemporalTable(temporalTableEntry);});
								// Step.5 Set current catalog and database. �� 
								Optional<String> catalog = environment.getExecution().getCurrentCatalog();// "current-catalog" ����
								Optional<String> database = environment.getExecution().getCurrentDatabase();// current-database ����
								database.ifPresent(tableEnv::useDatabase);
							}
						}
					}
				}
			}
		}
		
		openCli(sessionId, executor);{//SqlClient.openCli
			CliClient cli = new CliClient(sessionId, executor, historyFilePath)
			cli.open();{//CliClient.
				terminal.writer().append(CliStrings.MESSAGE_WELCOME);
				while (isRunning) {
					terminal.writer().append("\n");
					// ��ȡһ������; 
					String line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
					// 1. �����û���ѯ������� Calcite����,������Ĭ�� curCatalog,curDB���� FlinkCatalogReader;
					final Optional<SqlCommandCall> cmdCall = parseCommand(line);{//CliClient.
						parsedLine = SqlCommandParser.parse(executor.getSqlParser(sessionId), line);{
							Optional<SqlCommandCall> callOpt = parseByRegexMatching(stmt);
							if (callOpt.isPresent()) {//�����������; 
								return callOpt.get();
							}else{// û������, ��������; 
								return parseBySqlParser(sqlParser, stmt);{//SqlCommandParser.parseBySqlParser
									operations = sqlParser.parse(stmt);{//LocalExecutor.Parser������.parse()
										return context.wrapClassLoader(() -> parser.parse(statement));{// ParserImpl.parse()
											CalciteParser parser = calciteParserSupplier.get();
											FlinkPlannerImpl planner = validatorSupplier.get();
											SqlNode parsed = parser.parse(statement);
											Operation operation =SqlToOperationConverter.convert(planner, catalogManager, parsed)
											.orElseThrow(() -> new TableException("Unsupported query: " + statement));{// SqlToOperationConverter.convert()
												final SqlNode validated = flinkPlanner.validate(sqlNode);{// FlinkPlannerImpl.validate()
													val validator = getOrCreateSqlValidator();{
														val catalogReader = catalogReaderSupplier.apply(false);{
															PlannerContext.createCatalogReader(){
																SqlParser.Config sqlParserConfig = getSqlParserConfig();
																SqlParser.Config newSqlParserConfig =SqlParser.configBuilder(sqlParserConfig).setCaseSensitive(caseSensitive).build();
																SchemaPlus rootSchema = getRootSchema(this.rootSchema.plus());
																// ����� currentDatabase,currentDatabase ��Դ�� CatalogManager.����; Ӧ���Ǽ��� sql-client-defaults.yaml �����ɵ�; 
																// �� currentCatalog("myhive"), currentDatabase("default") ��ΪĬ�ϵ� SchemaPaths;
																List<List<String>> defaultSchemas = asList(asList(currentCatalog, currentDatabase), singletonList(currentCatalog));
																return new FlinkCalciteCatalogReader(CalciteSchema.from(rootSchema),defaultSchemas,typeFactory);
															}
														}
														validator = createSqlValidator(catalogReader)
													}
													validate(sqlNode, validator)
												}
												SqlToOperationConverter converter =new SqlToOperationConverter(flinkPlanner, catalogManager);
											}
											
										
										}
									}
									return new SqlCommandCall(cmd, operands);
								}
							}
						}
					}
					// 2. �ύִ��sql Calcite����; 
					cmdCall.ifPresent(this::callCommand);{
						switch (cmdCall.command) {
							case SET:
								callSet(cmdCall);
								break;
							case SELECT:
								callSelect(cmdCall);{//CliClient.callSelect()
									resultDesc = executor.executeQuery(sessionId, cmdCall.operands[0]);{//LocalExecutor.executeQuery()
										final ExecutionContext<?> context = getExecutionContext(sessionId);
										return executeQueryInternal(sessionId, context, query);{//LocalExecutor.
											final Table table = createTable(context, context.getTableEnvironment(), query);{
												return context.wrapClassLoader(() -> tableEnv.sqlQuery(selectQuery));{
													//TableEnvironmentImpl.sqlQuery(selectQuery);
												}
											}
											final DynamicResult<C> result =resultStore.createResult();
											pipeline = context.createPipeline(jobName);
											final ProgramDeployer deployer =new ProgramDeployer(configuration, jobName, pipeline, context.getClassLoader());
											deployer.deploy().get();
											return new ResultDescriptor();
										}
									}
									if (resultDesc.isTableauMode()) {
										tableauResultView =new CliTableauResultView();
									}
								}
								break;
							case INSERT_INTO:
							case INSERT_OVERWRITE:
								callInsert(cmdCall);
								break;
							case CREATE_TABLE:
								callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_CREATED);
								break;
						}
					}
				}
			}
		}
	}
}






