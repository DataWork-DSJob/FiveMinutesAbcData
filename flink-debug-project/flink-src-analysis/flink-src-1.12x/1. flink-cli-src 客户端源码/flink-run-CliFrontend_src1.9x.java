
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
		loadCustomCommandLine() -> new FlinkYarnSessionCli()
		
*/
CliFrontend.main(){
	// 1. find the configuration directory, 以此将 FLINK_CONF_DIR, ../conf, conf作为配置目录; 
	String configurationDirectory = getConfigurationDirectoryFromEnv();{
		String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		if (location != null) {
			return location;
		}else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_1;
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_2;
		}
	}
	
	// 2. load the global configuration; 就是先后按"#",':' 对目录下 flink-conf.yaml 切分解析k-v;
	Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);{
		File confDirFile = new File(configDir);
		File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);
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
		return configuration;
	}
	
	// 3. load the custom command lines; 反射获取 FlinkYarnSessionCli 对象成功,就加到 customCommandLines 中;
	List<CustomCommandLine<?>> customCommandLines = loadCustomCommandLines(Configuration configuration, String configurationDirectory);{
		try {
			String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
			customCommandLines.add(loadCustomCommandLine(flinkYarnSessionCLI,configuration, configurationDirectory, "y", "yarn"));{// CliFrontend.loadCustomCommandLine()
				Class<? extends CustomCommandLine> customCliClass = Class.forName(className).asSubclass(CustomCommandLine.class);
				//这里把从FLINK_CONF_DIR解析出的flinkConfig 配置,传给 FlinkYarnCli 
				Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types); new FlinkYarnSessionCli();{
					new FlinkYarnSessionCli(configuration,configurationDirectory, shortPrefix, longPrefix);{
						yarnApplicationIdFromYarnProperties = ConverterUtils.toApplicationId(yarnApplicationIdString);
						
						allOptions = new Options();
						allOptions.addOption(flinkJar);
						allOptions.addOption(jmMemory);
						
						File yarnPropertiesLocation = getYarnPropertiesLocation(yarnPropertiesFileLocation);
						
						this.yarnConfiguration = new YarnConfiguration();
					}
				}
			}
		} catch (NoClassDefFoundError | Exception e) {
			LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
		}
		customCommandLines.add(new DefaultCLI(configuration));
		return customCommandLines;
	}
	
	final CliFrontend cli = new CliFrontend(configuration, customCommandLines);{
		this.customCommandLineOptions = new Options();
		// 这里,把每个命令CmdLine的 通用参数和运行参数,都加入到  custom options选型中, 供下面解析命令行
		for (CustomCommandLine<?> customCommandLine : customCommandLines) {
			customCommandLine.addGeneralOptions(customCommandLineOptions);
			customCommandLine.addRunOptions(customCommandLineOptions);{
				FlinkYarnSessionCli.addGeneralOptions() 只加了1个 applicationId 选型;
				FlinkYarnSessionCli.addRunOptions() 把其 allOptions(共19个)都加进去; 
			}
		}
	}
	int retCode = SecurityUtils.getInstalledContext().runSecured(
		() -> cli.parseParameters(args){
			switch (action) {
				case ACTION_RUN: run(args);{// CliFrontend.run(String[] args)
					// 源码细节 如下 
				}
			}
		}
	);
	System.exit(retCode);
}










// flink-client_1.9x_src: run app 提交作业 

CliFrontend.run(String[] args){
	/* run 作业flink参数  
		flink资源配置: 	p parallelism, 
		依赖和包: 		  c class, C classpath, j jarfile, py python, pyfs pyFiles,  pym, 
		flink 状态配置: 	s fromSavepoint, n allowNonRestored,  sae shutdownOnAttactExit
		其他: 			a arguments, 
	*/
	Options commandOptions = CliFrontendParser.getRunCommandOptions(); // 16个参数: help,jarfile,class,parallelism, 
	
	/*添加customCommandLineOptions的21个Options: 
		flink资源配置:		ys yarnslots,yjm yarnjobManagerMemory, ytm yarntaskMgrMemory
		jobmanger, zkNamespace, detached, yarnname, 
		yarnjar,  yarnapplicationId
		yarncontainer, yarnqueue, yarnquery, 
		yarnstreaming,yarnnodeLabel, yarnhelp 
		其他:		yD 
	*/
	// 
	Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
	//把run后面的参数, 都解析成1各个 Option对象; 调用  commons-cli包的 DefaultParser 解析命令行 
	CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, true);{
		return parser.parse(options, args, stopAtNonOptions);{//org.apache.commons.cli.DefaultParser.parse()
			
		}
	}
	RunOptions runOptions = new RunOptions(commandLine);
	
	
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
	
	
	runProgram(customCommandLine, commandLine, runOptions, program);{//CliFrontend.runProgram()
		
		ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);{
			// 对于yarn 模式任务; 
			FlinkYarnSessionCli.createClusterDescriptor(CommandLine commandLine){
				Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
				return createDescriptor();{
					yarnClusterDescriptor.addShipFiles(shipFiles);
					
					// 基于yD 解析出动态参数,用'='平均成KV,并用'@@'把多个配置合成1个 字符串;
					Properties properties = cmd.getOptionProperties(dynamicproperties.getOpt());
					String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);
					yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);
					
				}
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



FlinkYarnSessionCli.createDescriptor(Configuration configuration, YarnConfiguration yarnConfiguration,
			String configurationDirectory, CommandLine cmd): AbstractYarnClusterDescriptor{
	
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




loadConfiguration:112, GlobalConfiguration (org.apache.flink.configuration)
loadConfiguration:82, GlobalConfiguration (org.apache.flink.configuration)
loadConfiguration:66, GlobalConfiguration (org.apache.flink.configuration)
<clinit>:130, JobGraphGenerator (org.apache.flink.optimizer.plantranslate)
createJobGraph:167, StreamingJobGraphGenerator (org.apache.flink.streaming.api.graph)
createJobGraph:94, StreamingJobGraphGenerator (org.apache.flink.streaming.api.graph)
getJobGraph:737, StreamGraph (org.apache.flink.streaming.api.graph)
getJobGraph:40, StreamingPlan (org.apache.flink.optimizer.plan)
execute:86, LocalStreamEnvironment (org.apache.flink.streaming.api.environment)
execute:1507, StreamExecutionEnvironment (org.apache.flink.streaming.api.environment)
pressureTest:73, FlinkDebugCommon (flink.debug)
testReuseObject:23, FlinkConfigurationDebug (flink.debug.configdebug)




