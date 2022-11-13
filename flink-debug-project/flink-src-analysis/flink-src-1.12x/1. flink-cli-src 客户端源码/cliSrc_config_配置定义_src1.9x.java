



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

// 本地执行时, createJobGraph() 构建执行逻辑
LocalStreamEnvironment.execute()
	// 创建执行计划时, 需要加载全局变量; 
	StreamGraph.getJobGraph() -> new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph()
		-> JobGraphGenerator.addUserArtifactEntries() -> GlobalConfiguration.loadConfiguration()
	// 启动Local的 MiniCluster时, 初始化默认配置时 
	MiniCluster.start() -> initializeIOFormatClasses(configuration) 
		-> FileOutputFormat.initDefaultsFromConfiguration(configuration); -> GlobalConfiguration.loadConfiguration()







// Network Config生成逻辑
// IDEA LoalEnv 模式


<init>:60, MiniClusterConfiguration (org.apache.flink.runtime.minicluster)
build:160, MiniClusterConfiguration$Builder (org.apache.flink.runtime.minicluster)
execute:105, LocalStreamEnvironment (org.apache.flink.streaming.api.environment)
execute:1507, StreamExecutionEnvironment (org.apache.flink.streaming.api.environment)
pressureTest:117, FlinkDebugCommon (flink.debug)

LocalStreamEnvironment.execute(StreamGraph streamGraph){
	
	Configuration configuration = new Configuration();
	// 把JobGraph的配置 都添加进, ? JobGraph.jobConfiguration 又从哪里来的? 
	configuration.addAll(jobGraph.getJobConfiguration());
	configuration.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "0");
	// add (and override) the settings with what the user defined
	configuration.addAll(this.configuration);
	int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());
	
	// 这里配置MiniCluster的配置,它来自 
	MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumSlotsPerTaskManager(numSlotsPerTaskManager)
			.build();{// MiniClusterConfiguration.build()
				Configuration modifiedConfiguration = new Configuration(configuration);
				modifiedConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlotsPerTaskManager);
				modifiedConfiguration.setString( RestOptions.ADDRESS, modifiedConfiguration.getString(RestOptions.ADDRESS, "localhost"));
				return new MiniClusterConfiguration(modifiedConfiguration, numTaskManagers,
					rpcServiceSharing, commonBindAddress);
			}
	MiniCluster miniCluster = new MiniCluster(cfg);
	
}




















/** Network IO 相关的Configuration & Option: 
* 最终Netty 配置封装类: org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration	, flink-core包 
* 定义Netty 配置参数Option类: org.apache.flink.configuration.NettyShuffleEnvironmentOptions, flink-core包
* 
*/


// Network Config生成逻辑
TaskManagerRunner.startTaskManager(configuration, ){
	TaskManagerServicesConfiguration taskManagerServicesConfiguration = TaskManagerServicesConfiguration.fromConfiguration(
				configuration,
				resourceID,
				remoteAddress,
				EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag(),
				EnvironmentInformation.getMaxJvmHeapMemory(),
				localCommunicationOnly);{
		ShuffleEnvironment<?, ?> shuffleEnvironment = createShuffleEnvironment();{//TaskManagerServices.createShuffleEnvironment()
			ShuffleEnvironmentContext shuffleEnvironmentContext = new ShuffleEnvironmentContext();
			ShuffleServiceLoader.loadShuffleServiceFactory(taskManagerServicesConfiguration.getConfiguration())
				.createShuffleEnvironment(shuffleEnvironmentContext);{//NettyShuffleServiceFactory.
					
					// 这里从 flink configuration 解析生成 NetworkShuffle的 配置;
					NettyShuffleEnvironmentConfiguration networkConfig = NettyShuffleEnvironmentConfiguration.fromConfiguration(
						shuffleEnvironmentContext.getConfiguration(), shuffleEnvironmentContext.getMaxJvmHeapMemory(),
						shuffleEnvironmentContext.isLocalCommunicationOnly(), shuffleEnvironmentContext.getHostAddress());{//NettyShuffleEnvironmentConfiguration.fromConfiguration()
						
						// 从taskmanager.memory.segment-size 读取pageSize大小, 默认32kb; 
						int pageSize = ConfigurationParserUtils.getPageSize(configuration);
						
						final int numberOfNetworkBuffers = calculateNumberOfNetworkBuffers(configuration, maxJvmHeapMemory);{
							// 如果有network.memory 4个参数(fraction,min,max, numberOfBuffers)任意一个, 就用新的network配置,走下面else逻辑; 
							if (!hasNewNetworkConfig(configuration)) {
								// fallback: number of network buffers
								numberOfNetworkBuffers = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
								checkOldNetworkConfig(numberOfNetworkBuffers);
							} else { // 重配了任意network参数, 走这里 
								if (configuration.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS)) {
									LOG.info("Ignoring old (but still present) network buffer configuration via {}.", NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key());
								}
								
								/* 通过可用Network内存大小, 算 buffer数量; IO性能最核心逻辑 
								*
								*/
								final long networkMemorySize = calculateNewNetworkBufferMemory(configuration, maxJvmHeapMemory); {
									final long jvmHeapNoNet; // Task总堆大小(前面基于  )
									final MemoryType memoryType = ConfigurationParserUtils.getMemoryType(config);
									if (memoryType == MemoryType.HEAP) {
										jvmHeapNoNet = maxJvmHeapMemory;
									} else if (memoryType == MemoryType.OFF_HEAP) {
										long configuredMemory = ConfigurationParserUtils.getManagedMemorySize(config) << 20; // megabytes to bytes
										if (configuredMemory > 0) {
											// The maximum heap memory has been adjusted according to configuredMemory, i.e.
											// maxJvmHeap = jvmHeapNoNet - configuredMemory
											jvmHeapNoNet = maxJvmHeapMemory + configuredMemory;
										} else {
											// The maximum heap memory has been adjusted according to the fraction, i.e.
											// maxJvmHeap = jvmHeapNoNet - jvmHeapNoNet * managedFraction = jvmHeapNoNet * (1 - managedFraction)
											jvmHeapNoNet = (long) (maxJvmHeapMemory / (1.0 - ConfigurationParserUtils.getManagedMemoryFraction(config)));
										}
									} else {
										throw new RuntimeException("No supported memory type detected.");
									}

									// finally extract the network buffer memory size again from:
									// jvmHeapNoNet = jvmHeap - networkBufBytes
									//              = jvmHeap - Math.min(networkBufMax, Math.max(networkBufMin, jvmHeap * netFraction)
									// jvmHeap = jvmHeapNoNet / (1.0 - networkBufFraction)
									float networkBufFraction = config.getFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION);// fraction 默认0.1 
									// jvmHeapNoNet / (1.0 - networkBufFraction) = jvmHeap堆总大小, 	jvmHeap * networkBufFraction = networkBufSize 网络内存大小;
									long networkBufSize = (long) (jvmHeapNoNet / (1.0 - networkBufFraction) * networkBufFraction);
									return calculateNewNetworkBufferMemory(config, networkBufSize, maxJvmHeapMemory); {
										float networkBufFraction = config.getFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION);
										long networkBufMin = MemorySize.parse(config.getString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN)).getBytes();
										long networkBufMax = MemorySize.parse(config.getString(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX)).getBytes();
										int pageSize = ConfigurationParserUtils.getPageSize(config);// segment-size = 32kb,
										// networkBufBytes = min(1G, max(64m, ? ))
										long networkBufBytes = Math.min(networkBufMax, Math.max(networkBufMin, networkBufSize));
										return networkBufBytes; // flink 1.9这里是从 堆内内存扣除; 
									}
								}
								// tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
								long numberOfNetworkBuffersLong = networkMemorySize / ConfigurationParserUtils.getPageSize(configuration);
								if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
									throw new IllegalArgumentException("The given number of memory bytes (" + networkMemorySize + ") corresponds to more than MAX_INT pages.");
								}
								numberOfNetworkBuffers = (int) numberOfNetworkBuffersLong;
							}
							return numberOfNetworkBuffers;
						}
						
						final NettyConfig nettyConfig = createNettyConfig(configuration, localTaskManagerCommunication, taskManagerAddress, dataport);
						int initialRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
						int maxRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);

						int buffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
						int extraBuffersPerGate = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);

						boolean isCreditBased = nettyConfig != null && configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_CREDIT_MODEL);

						boolean isNetworkDetailedMetrics = configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_DETAILED_METRICS);

						String[] tempDirs = ConfigurationUtils.parseTempDirectories(configuration);

						Duration requestSegmentsTimeout = Duration.ofMillis(configuration.getLong(NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));

						BoundedBlockingSubpartitionType blockingSubpartitionType = getBlockingSubpartitionType(configuration);

						boolean forcePartitionReleaseOnConsumption = configuration.getBoolean(NettyShuffleEnvironmentOptions.FORCE_PARTITION_RELEASE_ON_CONSUMPTION);

						return new NettyShuffleEnvironmentConfiguration();
		
					}
					
					return createNettyShuffleEnvironment(networkConfig);{
						
					}
			}
		}
	}
	
}




// LocalBufferPool 容量配置逻辑
Task.doRun(){
	setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);{
		// 正常1个输出, 对应1个ResultPartitionWriter, 可能有多个结果分区 
		for (ResultPartitionWriter partition : producedPartitions) {
			partition.setup();{
				BufferPool bufferPool = checkNotNull(bufferPoolFactory.apply(this));{
					int maxNumberOfMemorySegments = type.isBounded() ? numberOfSubpartitions * networkBuffersPerChannel + floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
					// If the partition type is back pressure-free, we register with the buffer pool for
					// callbacks to release memory.
					return bufferPoolFactory.createBufferPool(numberOfSubpartitions + 1, maxNumberOfMemorySegments,
						type.hasBackPressure() ? Optional.empty() : Optional.of(p));{
						
						this.numTotalRequiredBuffers += numRequiredBuffers;
						// We are good to go, create a new buffer pool and redistribute
						// non-fixed size buffers.
						LocalBufferPool localBufferPool =new LocalBufferPool(this, numRequiredBuffers, maxUsedBuffers, owner);
						allBufferPools.add(localBufferPool);
						
					}
				}
				
			}
		}
	}
}

















