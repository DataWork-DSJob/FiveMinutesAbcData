
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
		loadCustomCommandLine() -> new FlinkYarnSessionCli()
		
*/
CliFrontend.main(){
	// 1. find the configuration directory, �Դ˽� FLINK_CONF_DIR, ../conf, conf��Ϊ����Ŀ¼; 
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
	
	// 2. load the global configuration; �����Ⱥ�"#",':' ��Ŀ¼�� flink-conf.yaml �зֽ���k-v;
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
	
	// 3. load the custom command lines; �����ȡ FlinkYarnSessionCli ����ɹ�,�ͼӵ� customCommandLines ��;
	List<CustomCommandLine<?>> customCommandLines = loadCustomCommandLines(Configuration configuration, String configurationDirectory);{
		try {
			String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
			customCommandLines.add(loadCustomCommandLine(flinkYarnSessionCLI,configuration, configurationDirectory, "y", "yarn"));{// CliFrontend.loadCustomCommandLine()
				Class<? extends CustomCommandLine> customCliClass = Class.forName(className).asSubclass(CustomCommandLine.class);
				//����Ѵ�FLINK_CONF_DIR��������flinkConfig ����,���� FlinkYarnCli 
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
		// ����,��ÿ������CmdLine�� ͨ�ò��������в���,�����뵽  custom optionsѡ����, ���������������
		for (CustomCommandLine<?> customCommandLine : customCommandLines) {
			customCommandLine.addGeneralOptions(customCommandLineOptions);
			customCommandLine.addRunOptions(customCommandLineOptions);{
				FlinkYarnSessionCli.addGeneralOptions() ֻ����1�� applicationId ѡ��;
				FlinkYarnSessionCli.addRunOptions() ���� allOptions(��19��)���ӽ�ȥ; 
			}
		}
	}
	int retCode = SecurityUtils.getInstalledContext().runSecured(
		() -> cli.parseParameters(args){
			switch (action) {
				case ACTION_RUN: run(args);{// CliFrontend.run(String[] args)
					// Դ��ϸ�� ���� 
				}
			}
		}
	);
	System.exit(retCode);
}










// flink-client_1.9x_src: run app �ύ��ҵ 

CliFrontend.run(String[] args){
	/* run ��ҵflink����  
		flink��Դ����: 	p parallelism, 
		�����Ͱ�: 		  c class, C classpath, j jarfile, py python, pyfs pyFiles,  pym, 
		flink ״̬����: 	s fromSavepoint, n allowNonRestored,  sae shutdownOnAttactExit
		����: 			a arguments, 
	*/
	Options commandOptions = CliFrontendParser.getRunCommandOptions(); // 16������: help,jarfile,class,parallelism, 
	
	/*���customCommandLineOptions��21��Options: 
		flink��Դ����:		ys yarnslots,yjm yarnjobManagerMemory, ytm yarntaskMgrMemory
		jobmanger, zkNamespace, detached, yarnname, 
		yarnjar,  yarnapplicationId
		yarncontainer, yarnqueue, yarnquery, 
		yarnstreaming,yarnnodeLabel, yarnhelp 
		����:		yD 
	*/
	// 
	Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions, customCommandLineOptions);
	//��run����Ĳ���, ��������1���� Option����; ����  commons-cli���� DefaultParser ���������� 
	CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, true);{
		return parser.parse(options, args, stopAtNonOptions);{//org.apache.commons.cli.DefaultParser.parse()
			
		}
	}
	RunOptions runOptions = new RunOptions(commandLine);
	
	
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
	
	
	runProgram(customCommandLine, commandLine, runOptions, program);{//CliFrontend.runProgram()
		
		ClusterDescriptor<T> clusterDescriptor = customCommandLine.createClusterDescriptor(commandLine);{
			// ����yarn ģʽ����; 
			FlinkYarnSessionCli.createClusterDescriptor(CommandLine commandLine){
				Configuration effectiveConfiguration = applyCommandLineOptionsToConfiguration(commandLine);
				return createDescriptor();{
					yarnClusterDescriptor.addShipFiles(shipFiles);
					
					// ����yD ��������̬����,��'='ƽ����KV,����'@@'�Ѷ�����úϳ�1�� �ַ���;
					Properties properties = cmd.getOptionProperties(dynamicproperties.getOpt());
					String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);
					yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);
					
				}
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



FlinkYarnSessionCli.createDescriptor(Configuration configuration, YarnConfiguration yarnConfiguration,
			String configurationDirectory, CommandLine cmd): AbstractYarnClusterDescriptor{
	
	yarnClusterDescriptor.addShipFiles(shipFiles);
	
	// ����yD ��������̬����,��'='ƽ����KV,����'@@'�Ѷ�����úϳ�1�� �ַ���;
	final Properties properties = cmd.getOptionProperties(dynamicproperties.getOpt());{
		Properties props = new Properties();
		// ���������û������д���� options����, �����е� yD ȡ��(K,V)������ Prop��;
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
		if (value != null) { // key=value��'='�����ӳ��ַ���, 
			return Stream.of(key + dynamicproperties.getValueSeparator() + value);
		} else {
			return Stream.empty();
		}
	}).toArray(String[]::new);
	// 2��yD�����м���'@@' ˫@������, �е����; 
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




