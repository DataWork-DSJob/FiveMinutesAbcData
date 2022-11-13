


// YarnCli: 1. CliFrontend.main 线程中, 初始化 FlinkYarnSessionCli

// 1.2 flink-client_1.9x_src

CliFrontend.main() -> loadCustomCommandLines() -> loadCustomCommandLine(FlinkYarnSessionCli,configuration,"y", "yarn") 
	-> customCliClass.getConstructor(types); new FlinkYarnSessionCli()

return customCliClass.getConstructor(types); => new FlinkYarnSessionCli(Configuration configuration, String configurationDirectory, String shortPrefix, String longPrefix); {
	
	queue = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
	shipPath = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
	flinkJar = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
	jmMemory = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container with optional unit (default: MB)");
	tmMemory = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container with optional unit (default: MB)");
	container = new Option(shortPrefix + "n", longPrefix + "container", true, "Number of YARN container to allocate (=Number of Task Managers)");
	slots = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
	dynamicproperties = Option.builder(shortPrefix + "D")
		.argName("property=value")
		.numberOfArgs(2)
		.valueSeparator()
		.desc("use value for given property")
		.build();
	
	allOptions = new Options();
	allOptions.addOption(flinkJar);
	allOptions.addOption(jmMemory);
	allOptions.addOption(tmMemory);
	allOptions.addOption(slots);
	allOptions.addOption(dynamicproperties);
	allOptions.addOption(SHUTDOWN_IF_ATTACHED_OPTION);
	allOptions.addOption(YARN_DETACHED_OPTION);
	allOptions.addOption(name);
	allOptions.addOption(applicationId);
	
	this.yarnPropertiesFileLocation = configuration.getString(YarnConfigOptions.PROPERTIES_FILE_LOCATION);
	// Window: C:\Users\ALLENH~1\AppData\Local\Temp\.yarn-properties-allenhe888
	// Linux: 
	final File yarnPropertiesLocation = getYarnPropertiesLocation(yarnPropertiesFileLocation);
	if (yarnPropertiesLocation.exists()) {
		yarnPropertiesFile.load(is);
		yarnApplicationIdFromYarnProperties = ConverterUtils.toApplicationId(yarnApplicationIdString);
	} else {
		yarnApplicationIdFromYarnProperties = null;
	}
	// 这里就是加载默认的 Hdfs/ yarn 配置
	this.yarnConfiguration = new YarnConfiguration();{
		YarnConfiguration.static { // Yarn配置类 静态代码块
			addDeprecatedKeys();
			// 加载 yarn-default.xml 
			Configuration.addDefaultResource(YARN_DEFAULT_CONFIGURATION_FILE);{
				// Hadoop配置类 Configuration.defaultResources 中, 已包含 core-size.xml, core-default.xml, mapred-default.xml等6个配置文件;
				defaultResources.add(name); 
				for(Configuration conf : REGISTRY.keySet()) {
					if(conf.loadDefaults) {
						conf.reloadConfiguration(); {// Configuration.reloadConfiguration() 重新加载;
							properties = null;         // trigger reload
							finalParameters.clear();   // clear site-limits
						}
					}
				}
			}
			// 加载 yarn-site.xml 
			Configuration.addDefaultResource(YARN_SITE_CONFIGURATION_FILE);
			
		}
	}
}



// Hadoop Configuration 的生成和加载源码
- 默认把 core-default.xml,core-size.xml 等作为 Configuration.defaultResources 默认加载资源;
- 因为是从Classpath中加载defaultResources, 当CP中没有 hadoop默认配置时,就从hadoop-common包中加载相关默认配置;

// 1.3 hadoop-common-2.7.x_src 
org.apache.hadoop.conf.Configuration.get(name);{
	if (properties == null) {// 配置为空的话,重新加载 
		properties = new Properties();
		// 这里 this.resources 应该就是配置加载的
		loadResources(properties, this.resources, quietmode);{
			// 先从默认(core-default,core-size, hadoop-site中加载一遍);
			if(loadDefaults) { //默认=true, 加载默认
				// 默认 defaultResources 包括(core-default.xml,core-size.xml) ?
				for (String resource : defaultResources) {
					loadResource(properties, new Resource(resource), quiet);
				}
				if(getResource("hadoop-site.xml")!=null) {
					loadResource(properties, new Resource("hadoop-site.xml"), quiet);
				}
			}
			
			for (int i = 0; i < resources.size(); i++) {
				// 这里按文件加载 XML配置,并解析 DeprecationContext deprecations
				Resource ret = loadResource(properties, resources.get(i), quiet);{// Configuration.loadResource()
					DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
					if (resource instanceof URL) {   
					
					} else if (resource instanceof String) {        // a CLASSPATH resource
						URL url = getResource((String)resource);{
							// 默认从从类加载器,即ClassPath中 加载 core-default/yarn-site等配置;
							return classLoader.getResource(name);
						}
						doc = parse(builder, url); { //Configuration.parse() 利用XML解析器
							// systemId, Window IDEA的CP中没方 hadoop配置文件的话: jar:file:/D:/repo/mvnRepo/org/apache/hadoop/hadoop-common/2.7.2/hadoop-common-2.7.2.jar!/core-default.xml
							return (systemId == null) ? builder.parse(is) : builder.parse(is, systemId);
						}
					} 
					// 这次load的配置, 存放到 toAddTo: Properties 这里面; 它之间引用传进来的properties;
					Properties toAddTo = properties;
					NodeList props = root.getChildNodes();
					DeprecationContext deprecations = deprecationContext.get();
					for (int i = 0; i < props.getLength(); i++) {
						Node propNode = props.item(i);
						NodeList fields = prop.getChildNodes();
						
						DeprecatedKeyInfo keyInfo = deprecations.getDeprecatedKeyMap().get(attr);
						for (String key:keyInfo.newKeys) {
							loadProperty(toAddTo, name, key, value, finalParameter, source.toArray(new String[source.size()]));
						}
					}
					
				}
				resources.set(i, ret);
			}
		}
		if (overlay != null) {
			properties.putAll(overlay);
		}
	}
	return properties;
}












// 2 YarnCli: 2. Prepare to start AppMaster , FlinkYarnSessionCli 进程: "main"线程 构建并向yarnRM 发送 amContainer 

// Cli的main线程主要是创建YarnClient, 并最终构建1个 appContext: ApplicationSubmissionContext ,包含Java命令和相关jar/env变量等; 
	
	createClusterDescriptor(effectiveConfiguration); 链接Yarn RM,并建立RMClient;
		applyCommandLineOptionsToConfiguration(commandLine) 读cmdLine的 jm,tm,numSlots,ha4个参数到flinkConfiguraton中;
		createDescriptor() 		读cmdLine的 yD, 等参数到flinkConfig,并建立RMClient连接;
		
	yarnClusterDescriptor.deploySessionCluster() 
	yarnApplication = yarnClient.createApplication(); 创建Application
	// 创建 appContext,并调 yarnClient.submitApplication(appContext) 发给Yarn;
	startAppMaster(); // %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%
		* appContext = yarnApplication.getApplicationSubmissionContext(); 创建应用执行上下文 appCtx;
		* amContainer =setupApplicationMasterContainer(); 中创建 %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 格式的命令;
		* yarnClient.submitApplication(appContext); 将 appCtx发给RM/NM 进行远程Container/Java进程启动; 


CliFrontend.main(){
	cli.parseParameters(args);{
		switch (action) {
			case ACTION_RUN: run(params);{//CliFrontend.run()
				runProgram(customCommandLine, commandLine, runOptions, program);{
					
					ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);{
						// 对于yarn 模式任务; 
						FlinkYarnSessionCli.createClusterDescriptor(CommandLine commandLine){
								//从 命令行参数中, 如果有 hacluster-id, jmMem, tmMem, slots, 
							Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);{
								Configuration effectiveConfiguration = new Configuration(configuration);
								
								if (commandLine.hasOption(zookeeperNamespaceOption.getOpt())) {
									String zkNamespace = commandLine.getOptionValue(zookeeperNamespaceOption.getOpt());
									effectiveConfiguration.setString(HA_CLUSTER_ID, zkNamespace);
								}
								
								if (commandLine.hasOption(jmMemory.getOpt())) {
									effectiveConfiguration.setString(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, jmMemoryVal);
								}
								if (commandLine.hasOption(tmMemory.getOpt())) {
									effectiveConfiguration.setString(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, tmMemoryVal);
								}
								if (commandLine.hasOption(slots.getOpt())) {
									effectiveConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, Integer.parseInt(commandLine.getOptionValue(slots.getOpt())));
								}
								
								// 若无 -m --jobmanager 参数, 则使用 yarn配置文件中的 parallelism 参数;
								boolean canApplyYarnProperties = isYarnPropertiesFileMode(commandLine);{
									boolean canApplyYarnProperties = !commandLine.hasOption(addressOption.getOpt());
									
									return canApplyYarnProperties;
								}
								if (canApplyYarnProperties) {
									return applyYarnProperties(effectiveConfiguration); {
										String propParallelism = yarnPropertiesFile.getProperty(YARN_PROPERTIES_PARALLELISM);
										effectiveConfiguration.setInteger(CoreOptions.DEFAULT_PARALLELISM, Integer.parseInt(propParallelism));
										
										// handle the YARN client's dynamic properties
										String dynamicPropertiesEncoded = yarnPropertiesFile.getProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING);
										Map<String, String> dynamicProperties = getDynamicProperties(dynamicPropertiesEncoded);
										for (Map.Entry<String, String> dynamicProperty : dynamicProperties.entrySet()) {
											effectiveConfiguration.setString(dynamicProperty.getKey(), dynamicProperty.getValue());
										}
										return effectiveConfiguration;
									}
								} else {
									return effectiveConfiguration;
								}
							}
							
							return createDescriptor();{
								yarnClusterDescriptor.addShipFiles(shipFiles);
								
								// 基于yD 解析出动态参数,用'='平均成KV,并用'@@'把多个配置合成1个 字符串;
								Properties properties = cmd.getOptionProperties(dynamicproperties.getOpt());
								String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);
								yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);
								
							}
						}
					}
					
					client = clusterDescriptor.deployJobCluster();{
						return deployInternal(clusterSpecification, getYarnSessionClusterEntrypoint()); {// 源码详解 yarn: resourcenanger-src 
							isReadyForDeployment(clusterSpecification);
							// -Check if the specified queue exists 检查是否存在队列 
							checkYarnQueues(yarnClient);
							
							// -- Add dynamic properties to local flinkConfiguraton; 解析出动态参数,并添加到 flinkConfiguration作为Flink配置; 
							Map<String, String> dynProperties = getDynamicProperties(dynamicPropertiesEncoded);
							for (Map.Entry<String, String> dynProperty : dynProperties.entrySet()) {
								flinkConfiguration.setString(dynProperty.getKey(), dynProperty.getValue());
							}
							
							final YarnClientApplication yarnApplication = yarnClient.createApplication();
							final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();
							freeClusterMem = getCurrentFreeClusterResources(yarnClient);
							final int yarnMinAllocationMB = yarnConfiguration.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
							
							
							/** 关键源码: 构建和提交 AppMaster 
							*	这里定义AM进程的 环境变量, Classpath, Jar等; 
							* 	并提交给 Yarn-RM , x
							*/
							ApplicationReport report =startAppMaster(); //详细源码 见下文; 
							return () -> {return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());};
							
						}
					}
				}
			}
		}
	}
}



// 2.2 flink-yarn_1.9x_src: createDescriptor()   生成ClusterDescriptor集群作业 的各种配置参数 

FlinkYarnSessionCli.createDescriptor(Configuration configuration, YarnConfiguration yarnConfiguration,
			String configurationDirectory, CommandLine cmd): AbstractYarnClusterDescriptor{
	AbstractYarnClusterDescriptor yarnClusterDescriptor = getClusterDescriptor();

	// Jar Path,设置AM的Jar Path, 就是flink-yarn的路径, 默认用 yarnClusterDescriptor类所在的jar包,即flink-yarn 
	final Path localJarPath;
	if (cmd.hasOption(flinkJar.getOpt())) {
		String userPath = cmd.getOptionValue(flinkJar.getOpt());
		localJarPath = new Path(userPath);
	} else {
		// 如果没配 -j --jar参数指定, 就用 yarnClusterDescriptor类所在的jar包作为 am的jar(flink-yarn)包;
		LOG.info("No path for the flink jar passed. Using the location of "+ yarnClusterDescriptor.getClass() + " to locate the jar");
		String encodedJarPath = yarnClusterDescriptor.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
		decodedPath = URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
		localJarPath = new Path(new File(decodedPath).toURI());
	}
	yarnClusterDescriptor.setLocalJarPath(localJarPath);
	
	yarnClusterDescriptor.addShipFiles(shipFiles);
	
	// 基于yD 解析出动态参数,用'='平均成KV,并用'@@'把多个配置合成1个 字符串;
	final Properties properties = cmd.getOptionProperties(dynamicproperties.getOpt());{
		Properties props = new Properties();
		// 遍历所有用户命令行传入的 options对象, 把其中的 yD 取出(K,V)都存入 Prop中;
		for (Option option : options) {
			if (opt.equals(option.getOpt()) || opt.equals(option.getLongOpt())) {
				List<String> values = option.getValuesList();
				if (values.size() >= 2){
					props.put(values.get(0), values.get(1));
				} else if (values.size() == 1) {
					props.put(values.get(0), "true");
				}
			}
		}
		return props;
	}
	properties.stringPropertyNames().stream().flatMap((String key) -> {
		String value = properties.getProperty(key);
		if (value != null) { // key=value用'='号连接成字符串, 
			return Stream.of(key + dynamicproperties.getValueSeparator() + value);
		} else {
			return Stream.empty();
		}
	}).toArray(String[]::new);
	// 2个yD配置中间用'@@' 双@号连接, 有点搞了; 
	//historyserver.archive.fs.refresh-interval=10003@@taskmanager.heap.size=2008m
	String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);
	yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);
	
}









// 3 YarnCli_3_startAppMaster()  启动AM的完整源码 

/**	3  flinkCli-yarn-SubmitAM: deploySessionCluster()-> startAppMaster()-> yarnClient.submitApplication(appContext) 

YarnClusterDescriptor.startAppMaster(): 构建并启动 am: ApplicationMaster
	//1. 构建AM命令: YarnClusterDescriptor.setupApplicationMasterContainer(): 按照%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 构建AM命令;
	//2. 拼接CLASSPATH: YarnClusterDescriptor.startAppMaster()拼接$CLASSPATH,依次采用: $FLINK_CLASSPATH() + yarn.application.classpath ,其构成如下
			userClassPath(jobGraph.getUserJars(), pipeline.jars, usrlib) 
			* 	systemClassPaths = yarn.ship-files配置 + $FLINK_LIB_DIR变量下jars + logConfigFile;
			*	localResourceDescFlinkJar.getResourceKey() + jobGraphFilename + "flink-conf.yaml"
			yarn.application.classpath 默认采用: $HADOOP_CONF_DIR和 share下的common,hdfs,yar3个模块的目录;
	//3. 调用yarn api: YarnClientImpl.submitApplication() 与Yarn RM通信并提交启动 ApplicationMaster: YarnSessionClusterEntrypoint;
*/

// 3.1 flink-yarn_1.9x_src 
YarnClusterDescriptor.startAppMaster(){//YarnClusterDescriptor.startAppMaster()
	final FileSystem fs = FileSystem.get(yarnConfiguration);
	ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
	
	//1. 构建AM命令: YarnClusterDescriptor.setupApplicationMasterContainer(): 
	// 按照%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 构建AM命令;
	ContainerLaunchContext amContainer = setupApplicationMasterContainer(yarnClusterEntrypoint);{
		String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);// env.java.opts 的值
		Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		// 堆大小,等于 process - max(600, 0.25*process), 1G-> 424M;
		int heapSize = Utils.calculateHeapSize(jobManagerMemoryMb, flinkConfiguration);{
			float memoryCutoffRatio = conf.getFloat(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO);//containerized.heap-cutoff-ratio, 默认0.25f
			int minCutoff = conf.getInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN); // 600M
			// 一般根据进程内存  max(600M* 0.75f, 1G -> 0.75G
			int heapLimit = (int) ((float) memory * memoryCutoffRatio); // 1024 * 0.25 = 256M
			if (heapLimit < minCutoff) { // 256 < 600
				heapLimit = minCutoff; heapLimit = 600; 
			}
			return memory - heapLimit; // 1024 - 600 = 424M; 
		}
		String jvmHeapMem = String.format("-Xms%sm -Xmx%sm", heapSize, heapSize);
		
		startCommandValues.put("jvmopts", javaOpts);// env.java.opts  + env.java.opts.jobmanager 参数 
		startCommandValues.put("class", yarnClusterEntrypoint); // org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint
		
		startCommandValues.put("args", "");// 为什么? 
		
		String commandTemplate = flinkConfiguration.getString(
		ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,	// yarn.container-start-command-template 参数值 
				ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE); // 默认格式 "%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%" 
		
		//向上述命令格式中, 替换 %jvmmem%,%jvmopts%,"%args%" 内容, 拼接成 java命令; 
		// $JAVA_HOME/bin/java -Xms424m -Xmx424m  -Dlog.file="<LOG_DIR>/jobmanager.log" -Dlogback.configurationFile=file:logback.xml -Dlog4j.configuration=file:log4j.properties org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint  1> <LOG_DIR>/jobmanager.out 2> <LOG_DIR>/jobmanager.err
		String amCommand =BootstrapTools.getStartCommand(commandTemplate, startCommandValues);{
			for (Map.Entry<String, String> variable : startCommandValues.entrySet()) {
				template = template.replace("%" + variable.getKey() + "%", variable.getValue());
			}
			return template;
		}
		LOG.debug("Application Master start command: " + amCommand); // 有Debug参数; 
		return amContainer;
	}
	amContainer.setLocalResources(localResources);
	
	
	// Setup CLASSPATH and environment variables for ApplicationMaster
	// 2. 拼接生成CLASSPATH 
	Map<String, String> appMasterEnv = new HashMap<>();
	// FLINK_CLASSPATH 2: systemClassPaths= shipFiles(yarn.ship-files配置) + logConfigFile +systemShipFiles(Sys.FLINK_LIB_DIR变量) , 包括 localResources中上传的13个flink的lib下jar包;
	addLibFoldersToShipFiles(systemShipFiles);{
		String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);//从系统变量读取FLINK_LIB_DIR 的值;
		effectiveShipFiles.add(new File(libDir));
	}
	for (String classPath : systemClassPaths) classPathBuilder.append(classPath).append(File.pathSeparator);
	// FLINK_CLASSPATH 3: 
	classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
	classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
	classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
	//FLINK_CLASSPATH 6: include-user-jar=last时, 把userClassPath 的jars加到CP后面; 
	if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) classPathBuilder.append(userClassPath).append(File.pathSeparator);
	
	appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());
	appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES,fileUploader.getApplicationDir().toUri().toString());
	// 设置 CLASSPATH的参数, org.apache.flink.yarn.Utils 代码限制了 只能相同 系统才能提交; 
	Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);{
		// 1. 先把 _FLINK_CLASSPATH中 lib中13个flink相关jar包加到CP
		addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));{//org.apache.flink.yarn.Utils.addToEnvironment()
			String val = environment.get(variable); // 从 appMasterEnv已有变量中查 CLASSPATH , 
			//将 value($_FLINK_CLASSPATH变量内容) 内容添加进去; 
			// Window-Idea,value内容为: "flink-examples-batch_2.11-1.9.3.jar;log4j.properties;logback.xml;flink.jar;flink-conf.yaml;job.graph;"
			if (val == null) {
				val = value;
			} else {
				// 代码的关键, File.pathSeparator , 执行拼接 CP的代码是在Window本地的 CliFrontend进程内, 符号是";"
				// 但是到了Linux系统, 相应分割符号是":", 这串CP就无法 识别了; 
				val = val + File.pathSeparator + value;
			}
			
		}
		
		// 2. yarn.application.classpath + 
		String[] applicationClassPathEntries = conf.getStrings(
					YarnConfiguration.YARN_APPLICATION_CLASSPATH,
					YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);{
			String valueString = get(name);// 获取yarn.application.classpath 变量
			if (valueString == null) {// 采用Yarn默认CP: YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH, 包括7个;
				return defaultValue;// 默认YarnCP包括4类: CONF_DIR和 share下的common,hdfs,yar3个模块的目录;
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



// 3.2 flink yarn CLASSPATH 定义
/* 2. 定义$FLINK_CLASSPATH, CLASSPATH
	List<Path> providedLibDirs:		从 yarn.provided.lib.dirs 中读取配置	未配置则未空;
	
$FLINK_CLASSPATH 
	systemClassPaths = yarn.ship-files配置 + $FLINK_LIB_DIR变量下jars + logConfigFile
		- fileUploader.providedSharedLibs 中的 非dist非plugin 部分;
			* List<Path> providedLibDirs: yarn.provided.lib.dirs属性中有效dir部分
		- uploadedDependencies: 仅添加systemShipFiles中 非PUBLIc&& 非dist 的部分;
			- systemShipFiles
				- logConfigFilePath
				- $FLINK_LIB_DIR, 当 providedLibDirs(yarn.provided.lib.dirs) 为空时, 才添加 $FLINK_LIB_DIR
		- userClassPaths:  仅当yarn.per-job-cluster.include-user-jar=order时, 添加 userJarFiles
			- userJarFiles:	
				* JobGraph.userJars 	pipeline.jars(args[0]/-j/--jarfile指定) ,如examples/batch/WordCount.jar
				* jarUrls:				pipeline.jars:		存在且当 YarnApplication模式时, 才被加到 userJarFiles 中, 一般是 PerJob/YarnSession 所以不加入 CP;


	userClassPath: 		取[非PUBLIc &&非dist]的userJarFiles;
		- userJarFiles:	 
			- JobGraph.userJars 	pipeline.jars(args[0]/-j/--jarfile指定) ,如examples/batch/WordCount.jar
			- pipeline.jars:		存在且当 YarnApplication模式时, 才被加到 userJarFiles 中, 一般是 PerJob/YarnSession 所以不加入 CP;
			
	
	flinkJarPath: (yarn.flink-dist-jar 或 this.codesource.localpath
		yarn.flink-dist-jar
			若不存在,则使用 this.codesource.localpath(即flink-dist本包)
	
	localResourceDescFlinkJar.getResourceKey()
	jobGraphFilename
	"flink-conf.yaml"
	
yarn.application.classpath
	$HADOOP_CONF_DIR
	common: $HADOOP_COMMON_HOME/share/*/common/*
	hdfs: $HADOOP_HDFS_HOME/share/*/hdfs/*
	yarn: $HADOOP_YARN_HOME/share/*/yarn/*
	
*/


// flink-1.9 拼接Yarn AM的CP相关问题
	- Utils.addToEnvironment() 中使用 File.pathSeparator 作为两个CP路径的分隔符, 如果Cli是在Window执行(";"),则生成的CP在Linux Hadoop无法执行; 
	- Spark是如何解决的呢? 先都丢到临时包里, 到时再把临时包一起上传解压, 到时再拼接CP ?
	- 社区有其他人提了,没解决  https://issues.apache.org/jira/browse/FLINK-17858













// #4  YarnCli_4_startAppMaster_submitApplication()  启动AM中 提交AM的源码?

/** 4  yarnCli: submitApplication 与Yarn RM通信,提交启动AM; 
	//1. 发送Rpc请求: ProtobufRpcEngine.invoke()
	//2. 通信等待非 waitingStates就结束阻塞,返回 applicationId
*/

YarnClientImpl.submitApplication(ApplicationSubmissionContext appContext){//YarnClientImpl.submitApplication()
	SubmitApplicationRequest request =Records.newRecord(SubmitApplicationRequest.class);
	request.setApplicationSubmissionContext(appContext);
	
	rmClient.submitApplication(request);{// ApplicationClientProtocolPBClientImpl.submitApplication()
		// yarn 的resourceManager的 resourcemanager.ClientRMService 进行处理
		SubmitApplicationRequestProto requestProto= ((SubmitApplicationRequestPBImpl) request).getProto();
		SubmitApplicationResponseProto proto= proxy.submitApplication(null,requestProto){
			// 实际执行: 
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
	while (true) {// 非waitingStates 就跳出返回 applicationId
		if (!waitingStates.contains(state)) {
			LOG.info("Submitted application " + applicationId);
			break;
		}
	}
	return applicationId;
}













