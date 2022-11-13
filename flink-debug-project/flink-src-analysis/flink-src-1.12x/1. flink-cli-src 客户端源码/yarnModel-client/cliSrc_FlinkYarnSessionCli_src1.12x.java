


// YarnCli: 1. CliFrontend.main 线程中, 初始化 FlinkYarnSessionCli
// flink-client_1.12x_src
?



// Hadoop Configuration 的生成和加载源码
- 默认把 core-default.xml,core-size.xml 等作为 Configuration.defaultResources 默认加载资源;
- 因为是从Classpath中加载defaultResources, 当CP中没有 hadoop默认配置时,就从hadoop-common包中加载相关默认配置;

// hadoop-common-2.7.x_src 
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
// YarnClusterDescriptor.startAppMaster() 中创建 appContext,并调 yarnClient.submitApplication(appContext) 发给Yarn;
// %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%

FlinkYarnSessionCli.run()
	- yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration); 链接Yarn RM,并建立RMClient;
	- yarnClusterDescriptor.deploySessionCluster() 
	- yarnApplication = yarnClient.createApplication(); 创建Application
	- startAppMaster(); 
		* appContext = yarnApplication.getApplicationSubmissionContext(); 创建应用执行上下文 appCtx;
		* amContainer =setupApplicationMasterContainer(); 中创建 %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 格式的命令;
		* yarnClient.submitApplication(appContext); 将 appCtx发给RM/NM 进行远程Container/Java进程启动; 

startAppMaster:995, AbstractYarnClusterDescriptor (org.apache.flink.yarn)
deployInternal:509, AbstractYarnClusterDescriptor (org.apache.flink.yarn)
deployJobCluster:75, YarnClusterDescriptor (org.apache.flink.yarn)
runProgram:230, CliFrontend (org.apache.flink.client.cli)

// 2.2 flink-yarn_1.12x_src: deployAndPrepareStartAM 

FlinkYarnSessionCli.main(){
	final String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
	final FlinkYarnSessionCli cli = new FlinkYarnSessionCli(flinkConfiguration,configurationDirectory,"");
	retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));{//FlinkYarnSessionCli.run(){
		final CommandLine cmd = parseCommandLineOptions(args, true);
		// 主要耗时1:  链接Yarn ResurceManager
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
								}else{// 非HA, 这里; 
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
			// 主要耗时2: 发布应用; 
			clusterClientProvider = yarnClusterDescriptor.deploySessionCluster(clusterSpecification);{//YarnClusterDescriptor.deploySessionCluster()
				return deployInternal(clusterSpecification, getYarnSessionClusterEntrypoint()); {// 源码详解 yarn: resourcenanger-src 
					isReadyForDeployment(clusterSpecification);
					checkYarnQueues(yarnClient);
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


// flink yarn CLASSPATH 定义
{
	
	/* 定义 
		jobGraph.userJars:		pipeline.jars:			来自 args[0] or -j or --jarfile 参数指定
		jobGraph.classpaths:	pipeline.classpaths:	来自 -C or --classpath 参数指定
	*/
	CliFrontend.run(){
		final List<URL> jobJars = getJobJarAndDependencies(programOptions);{
			// entryPointClass 就是 -c/--class参数指定值; 
			String entryPointClass = programOptions.getEntryPointClassName(); //未指定未空;
			// jarFilePath 即App Jar包, 默认 args[0] 或 -j 或 --jarfile 指定;
			String jarFilePath = programOptions.getJarFilePath();
			File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;
			return PackagedProgram.getJobJarAndDependencies(jarFile, entryPointClass);{
				URL jarFileUrl = loadJarFile(jarFile);
			}
		}
		Configuration effectiveConfiguration =getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);{
			ExecutionConfigAccessor executionParameters = ExecutionConfigAccessor.fromProgramOptions(checkNotNull(programOptions), checkNotNull(jobJars));{
					final Configuration configuration = new Configuration();
					// 向config中写入 execution.attached,pipeline.classpaths等4个变量;
					options.applyToConfiguration(configuration);{
						// 来自 --classpath or -C 参数的指定;
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
			// 从config中读取 pipeline.jars 作为变量;
			List<URL> jarFilesToAttach = executionConfigAccessor.getJars();{
				return ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URL::new);//取pipeline.jars变量;
			}
			jobGraph.addJars(jarFilesToAttach);{
				for (URL jar : jarFilesToAttach) {
					addJar(new Path(jar.toURI()));{//往 jobGraph.userJars:List<Path> 中添加 pipeline.jars变量的有效URI
						if (!userJars.contains(jar)) {
							userJars.add(jar);
						}
					}
				}
			}
			//读取pipeline.classpaths 的变量值,并赋值 jobGraph.classpaths: List<URL> 
			jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
		}
		clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);
	}
	
	/* 1. 定义 dist,ship,archives资源路径: 
		flinkJarPath:	从 yarn.flink-dist-jar配置,或者将 this.codesource本包作为 dist包路径; 	未配置默认: /opt/flink/flink-1.12.2/lib/flink-dist_2.11-1.12.2.jar
		shipFiles:		从 yarn.ship-files读取			未配默认为空;
		shipArchives:	从 yarn.ship-archives 读取 		未配默认为空;
	*/
	AbstractJobClusterExecutor.execute().createClusterDescriptor().getClusterDescriptor(){
		final YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration); yarnClient.start();
		
		new YarnClusterDescriptor();{
			this.userJarInclusion = getUserJarInclusionMode(flinkConfiguration);
			//1 从 yarn.flink-dist-jar配置,或者将 this.codesource本包作为 dist包路径,并赋值 YarnClusterDescriptor.flinkJarPath 变量;
			getLocalFlinkDistPath(flinkConfiguration).ifPresent(this::setLocalJarPath);{
				String localJarPath = configuration.getString(YarnConfigOptions.FLINK_DIST_JAR); // yarn.flink-dist-jar
				if (localJarPath != null) {
					return Optional.of(new Path(localJarPath));
				}
				final String decodedPath = getDecodedJarPath();{//从 Class.pd.codesource.location.path //this的类就是 flink-dist.jar导进的;
					final String encodedJarPath =getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
					return URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
				}
				return decodedPath.endsWith(".jar")? Optional.of(new Path(new File(decodedPath).toURI())): Optional.empty();
			}
			//2 从 yarn.ship-files读取 资源文件路径,并赋值 YarnClusterDescriptor.shipFiles 变量;
			decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_FILES){// YarnClusterDescriptor.decodeFilesToShipToCluster
				final List<File> files =ConfigUtils.decodeListFromConfig(configuration, configOption, File::new);// yarn.ship-files 定义 ship:jar包船?
				return files.isEmpty() ? Optional.empty() : Optional.of(files);
			}.ifPresent(this::addShipFiles);
			//3 从 yarn.ship-archives 读取 资源文件路径,并赋值 YarnClusterDescriptor.shipArchives 变量;
			decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_ARCHIVES).ifPresent(this::addShipArchives);
			this.yarnQueue = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_QUEUE);
		}
	}
	

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

	YarnClusterDescriptor.startAppMaster(){//YarnClusterDescriptor.startAppMaster()
		final FileSystem fs = FileSystem.get(yarnConfiguration);
		ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
		// 从 yarn.provided.lib.dirs 中读取配置;若存在则加到 systemClassPaths->CLASSPATH; 若不存在,则会启用加载$FLINK_LIB_DIR到systemClassPaths(->CP);
		final List<Path> providedLibDirs =Utils.getQualifiedRemoteSharedPaths(configuration, yarnConfiguration);{
			return getRemoteSharedPaths(){//Utils.
				// yarn.provided.lib.dirs
				final List<Path> providedLibDirs =ConfigUtils.decodeListFromConfig(configuration, YarnConfigOptions.PROVIDED_LIB_DIRS, strToPathMapper);
				return providedLibDirs;
			}
		}
		//重点是过滤 providedLibDirs(yarn.provided.lib.dirs) 中 为dir目录的,并赋值给 fileUploader.providedSharedLibs
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
		
		// 若shipFiles有(yarn.ship-files 或空), 添加到 systemShipFiles中,并最终 -> uploadedDependencies -> systemClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		Set<File> systemShipFiles = new HashSet<>(shipFiles.size());
		for (File file : shipFiles) {
			 systemShipFiles.add(file.getAbsoluteFile());
		}
		// $internal.yarn.log-config-file ,如果存在则加到 systemShipFiles中; 一般这里是: /opt/flink/conf/log4j.properties; 最终 -> uploadedDependencies -> systemClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		final String logConfigFilePath =configuration.getString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
		if (null !=logConfigFilePath) {
			systemShipFiles.add(new File(logConfigFilePath));
		}
		// 若 yarn.provided.lib.dirs不存在, 则加载 FLINK_LIB_DIR 到 systemShipFiles -> systemClassPaths -> CLASSPATH;
		if (providedLibDirs == null || providedLibDirs.isEmpty()) {
			addLibFoldersToShipFiles(systemShipFiles);{//YarnClusterDescriptor.addLibFoldersToShipFiles()
				String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);// 即 $FLINK_LIB_DIR 环境变量;
				if (libDir != null) {
					File directoryFile = new File(libDir);
					if (directoryFile.isDirectory()) {
						effectiveShipFiles.add(directoryFile);//effectiveShipFiles 即传入的 systemShipFiles;
					}
				}else if(shipFiles.isEmpty()){// 如果 null == libDir($FLINK_LIB_DIR为空),而 shipFiles(yarn.ship-files)也为空,则warn告警;
					LOG.warn("Environment variable 'FLINK_LIB_DIR' not set and ship files have not been provided manually. Not shipping any library files");
				}
			}
		}
		
		final Set<Path> userJarFiles = new HashSet<>();
		// 将JobGraph.userJars添加到 userJarFiles中; 	应该就是 -jar指定的App包,如examples/batch/WordCount.jar;  并最终 userJarFiles-> userClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		if (jobGraph != null) {
			List<Path> jobUserJars = jobGraph.getUserJars().stream().map(f -> f.toUri()).map(Path::new).collect(Collectors.toSet());
			userJarFiles.addAll(jobUserJars);
		}
		//从 pipeline.jars中读取值并赋值给 jarUrls;  默认就是-jar 路径: examples/batch/WordCount.jar
		final List<URI> jarUrls =ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URI::create);// 从pipeline.jars读取值;
		//只有当 YarnApplication 模式时,才会加到 userClassPaths ->$FLINK_CLASSPATH中;  一般 yarnClusterEntrypoint是 YarnJob or YarnSession, 所以不加入 CP;
		if (jarUrls != null && YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)) {
			userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
		}
		
		// Register all files in provided lib dirs as local resources with public visibility and upload the remaining dependencies as local resources with APPLICATION visibility.
		// 把fileUploader.providedSharedLibs( yarn.provided.lib.dirs属性中有效dir部分) 中的 非dist非plugin的, 
		final List<String> systemClassPaths = fileUploader.registerProvidedLocalResources();{// YarnApplicationFileUploader.registerProvidedLocalResources()
			final ArrayList<String> classPaths = new ArrayList<>();
			providedSharedLibs.forEach((fileName, fileStatus)->{
				final Path filePath = fileStatus.getPath();
				if (!isFlinkDistJar(filePath.getName()) && !isPlugin(filePath)) {// 把非dist非plugin的依赖文件,添加到 classPaths中;
					classPaths.add(fileName);
				}else if (isFlinkDistJar(filePath.getName())) { // 如果是flink-dist文件,直接赋值给 flinkDist;
					flinkDist = descriptor;
				}
			});
		}
		// 将systemShipFiles中(logConfigFile + $FLINK_LIB_DIR(不存在yarn.provided.lib.dirs时) )内容赋给 shipFiles;
		Collection<Path> shipFiles = systemShipFiles.stream().map(e -> new Path(e.toURI())).collect(Collectors.toSet());
		// 将shipFiles中(1或2项)所有(递归遍历)内容,过滤出 [PUBLIC] && 非dist] 的所有 archives & resources, 一起返回赋给uploadedDependencies;
		final List<String> uploadedDependencies =fileUploader.registerMultipleLocalResources(shipFiles,Path.CUR_DIR,LocalResourceType.FILE);{
			// 解析shipFiles中,所有目录的所有的文件,都加载到 localPath中;
			final List<Path> localPaths = new ArrayList<>();
			for (Path shipFile : shipFiles) {
				if (Utils.isRemotePath(shipFile.toString())) {
					
				}else{
					final File file = new File(shipFile.toUri().getPath());
					if (file.isDirectory()) {// 
						Files.walkFileTree();//把目前下所有配置都加载?
					}
				}
				localPaths.add(shipFile);
				relativePaths.add(new Path(localResourcesDirectory, shipFile.getName()));
			}
			// 只有 非dist( !isFlinkDistJar ) 且 非Publich ( !alreadyRegisteredAsLocalResource() ) 的filePath才会被添加;
			for (int i = 0; i < localPaths.size(); i++) {
				if (!isFlinkDistJar(relativePath.getName())) {
					// 往YarnApplicationFileUploader 中 remotePath,envShipResourceList,localResources等变量 添加;
					YarnLocalResourceDescriptor resourceDescriptor =registerSingleLocalResource(key,loalpath,true,true);
					if (!resourceDescriptor.alreadyRegisteredAsLocalResource(){// 只有非PUBLIC公开级别的资源 才添加; log4j.properties因为是APP级别被过滤掉;
						return this.visibility.equals(LocalResourceVisibility.PUBLIC)
					}) {
						if (key.endsWith("jar")) { //是jar的算到 archives,
							archives.add(relativePath.toString());
						}else{ //所有非jar的file 都算到 resource中; 
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
			// 取$FLINK_PLUGINS_DIR环境值,或采用默认"plugins" 作为 为插件目录并添加到shipOnlyFiles中;
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
				pluginsDir.ifPresent(effectiveShipFiles::add);// 如果plugins配置存在,则 add (到shipOnlyFiles),并返回;
			}
			fileUploader.registerMultipleLocalResources(); // 
		}
		if (!shipArchives.isEmpty()) {//若yarn.ship-archives不为空,
			shipArchivesFile = shipArchives.stream().map(e -> new Path(e.toURI())).collect(Collectors.toSet());
			fileUploader.registerMultipleLocalResources(shipArchivesFile);
		}
		
		// localResourcesDir= "."
		String localResourcesDir= userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR, LocalResourceType.FILE;
		final List<String> userClassPaths =fileUploader.registerMultipleLocalResources(userJarFiles, localResourcesDir);{// 过滤其中[非PUBLIC] && 非dist] 
			for (int i = 0; i < localPaths.size(); i++) {
				final Path relativePath = localPaths.get(i).get(i);
				if (!isFlinkDistJar(relativePath.getName())) {
					// 只要不是PUBLIC 级别的, 就添加; 这里的 userJar(如:examples/batch/WordCount.jar) 被成功添加;
					if (!resourceDescriptor.alreadyRegisteredAsLocalResource(){// 只要非PUBLIC公开级别的, 就添加; 
						return this.visibility.equals(LocalResourceVisibility.PUBLIC)
					}) {
						if (key.endsWith("jar")) { //是jar的算到 archives,
							archives.add(relativePath.toString());
						}else{ //所有非jar的file 都算到 resource中; 
							resources.add(relativePath.getParent().toString());
						}
					}
				}
			}
		}
		// 当yarn.per-job-cluster.include-user-jar=order时, 添加userClassPaths到 systemClassPath
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.ORDER) {//yarn.per-job-cluster.include-user-jar=order时 
			systemClassPaths.addAll(userClassPaths);
		}
		
		//FLINK_CLASSPATH 1: include-user-jar=first时,把 jobGraph.getUserJars() &pipeline.jars &usrlib 目录下jars 加到前面;
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST){////yarn.per-job-cluster.include-user-jar=first时, userClassPath放前面;
			classPathBuilder.append(userClassPath).append(File.pathSeparator);
		} 
		Collections.sort(systemClassPaths);
		Collections.sort(userClassPaths);
		StringBuilder classPathBuilder = new StringBuilder();
		
		for (String classPath : systemClassPaths) {// 添加system级别的CP
            classPathBuilder.append(classPath).append(File.pathSeparator);
        }
		// 封装 flinkJarPath(yarn.flink-dist-jar 或 this.codesource.localpath本包,即flink-dist包); 并添加到 classPath中;
		final YarnLocalResourceDescriptor localResourceDescFlinkJar =fileUploader.uploadFlinkDist(flinkJarPath);
		classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
		
		// 把jobGraph序列号成文件,并把 "job.graph" 添加到classpath;
		if (jobGraph != null) {
			File tmpJobGraphFile = File.createTempFile(appId.toString(), null);
			// 把jobGraph对象写出到临时文件: /tmp/application_1639998011452_00604014191052203287620.tmp
			try (FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
				 ObjectOutputStream obOutput = new ObjectOutputStream(output)) {
                    obOutput.writeObject(jobGraph);
            }
			final String jobGraphFilename = "job.graph";
			configuration.setString(JOB_GRAPH_FILE_PATH, jobGraphFilename);
			fileUploader.registerSingleLocalResource();
			classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
		}
		// 把"flink-conf.yaml" 添加到classPath中;
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
		// 设置 CLASSPATH的参数
		Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);{
			// 1. 先把 _FLINK_CLASSPATH中 lib中13个flink相关jar包加到CP
			addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
			// 2. yarn.application.classpath + 
			String[] applicationClassPathEntries =conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);{
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

}














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













