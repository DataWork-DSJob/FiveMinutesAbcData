



/** Network IO 相关的Configuration & Option: 
* 最终Netty 配置封装类: org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration	, flink-core包 
* 定义Netty 配置参数Option类: org.apache.flink.configuration.NettyShuffleEnvironmentOptions, flink-core包
* 
*/

// flink-runtime_src1.12x_taskmanager: TM中 Shuffle Netty 的配置加载和生成策略 

TaskManagerRunner.startTaskManager(configuration, ){
	
	TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);{
		checkTaskExecutorResourceConfigSet(config);// 检查配置是否存在?
		// taskmanager.cpu.cores, 从哪里配的?
		// 这里, 取network.min(默认64M)作为 networkMemorySize
		return new TaskExecutorResourceSpec( new CPUResource(config.getDouble(TaskManagerOptions.CPU_CORES)),
                config.get(TaskManagerOptions.TASK_HEAP_MEMORY),		// task.heap.size		堆内 
                config.get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY),	// task.off-heap.size	堆外 
                config.get(TaskManagerOptions.NETWORK_MEMORY_MIN),		// network.min			网络.min  64M
                config.get(TaskManagerOptions.MANAGED_MEMORY_SIZE));	// managed.size			
				
	}
	
	// 生成TM的配置 
	TaskManagerServicesConfiguration taskManagerServicesConfiguration = TaskManagerServicesConfiguration.fromConfiguration(
                        configuration, resourceID, externalAddress, localCommunicationOnly,
                        taskExecutorResourceSpec);{
		// 创建Shuffle 环境
		ShuffleEnvironment<?, ?> shuffleEnvironment = createShuffleEnvironment();{//TaskManagerServices.createShuffleEnvironment()
			ShuffleEnvironmentContext shuffleEnvironmentContext = new ShuffleEnvironmentContext();
			
			return ShuffleServiceLoader.loadShuffleServiceFactory(taskManagerServicesConfiguration.getConfiguration())
                .createShuffleEnvironment(shuffleEnvironmentContext);{//NettyShuffleServiceFactory.
					// 这里从 flink configuration 解析生成 NetworkShuffle的 配置;
					NettyShuffleEnvironmentConfiguration networkConfig = NettyShuffleEnvironmentConfiguration.fromConfiguration(
						shuffleEnvironmentContext.getConfiguration(), shuffleEnvironmentContext.getMaxJvmHeapMemory(),
						shuffleEnvironmentContext.isLocalCommunicationOnly(), shuffleEnvironmentContext.getHostAddress());
					
					return createNettyShuffleEnvironment(networkConfig);{
						
					}
					
			}
			
		}
		final int listeningDataPort = shuffleEnvironment.start();

        final KvStateService kvStateService = KvStateService.fromConfiguration(taskManagerServicesConfiguration);
        kvStateService.start();
		
	}
	
	TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
                        taskManagerServicesConfiguration,
                        blobCacheService.getPermanentBlobService(),
                        taskManagerMetricGroup.f1,
                        ioExecutor,
                        fatalErrorHandler);
	
}



NettyShuffleServiceFactory.createShuffleEnvironment(ShuffleEnvironmentContext shuffleEnvironmentContext){//NettyShuffleServiceFactory.createShuffleEnvironment()
					
	// 这里从 flink configuration 解析生成 NetworkShuffle的 配置;
	NettyShuffleEnvironmentConfiguration networkConfig = NettyShuffleEnvironmentConfiguration.fromConfiguration(
		shuffleEnvironmentContext.getConfiguration(), shuffleEnvironmentContext.getMaxJvmHeapMemory(),
		shuffleEnvironmentContext.isLocalCommunicationOnly(), shuffleEnvironmentContext.getHostAddress());{//NettyShuffleEnvironmentConfiguration.fromConfiguration()
		
		// 从taskmanager.memory.segment-size 读取pageSize大小, 默认32kb; 
		int pageSize = ConfigurationParserUtils.getPageSize(configuration);
		
		final NettyConfig nettyConfig = createNettyConfig(configuration, localTaskManagerCommunication, taskManagerAddress, dataport);
		
		// 用上面的 networkMemorySize(默认64M) / pageSize(默认32kb)  = 默认 2048 个buffers 
		final int numberOfNetworkBuffers = calculateNumberOfNetworkBuffers(configuration, maxJvmHeapMemory);{
			logIfIgnoringOldConfigs(configuration); // numberOfBuffers 参数已经废弃, 如果使用了就log提醒
			// 前面算出 networkMemorySize = ? = 64M 
			long numberOfNetworkBuffersLong = networkMemorySize.getBytes() / pageSize;
			return (int) numberOfNetworkBuffersLong;
		}
		// request-backoff.initial, 默认100, 指定input channels的partition requests的最小backoff时间(毫秒), 会影响什么?
		int initialRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_INITIAL);
        // request-backoff.max, 默认10000/1w, input channels的partition requests的最大backoff时间; 调大可用增加容错时间; 
		// 相应的 akka 的 timeout 调大一点 
		int maxRequestBackoff = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_REQUEST_BACKOFF_MAX);
        // memory.buffers-per-channel, 每个outgoing/incoming channel使用buffers数量，默认为2; 调大可增加性能 
		// Number of exclusive network buffers for each outgoing/incoming channel, at least 2 for good performance
		int buffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        // floating-buffers-per-gate, 默认8个, Number of extra network buffers to use for each gate , how many floating credits are shared among all the input channels
		int extraBuffersPerGate = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
		// max-buffers-per-channel, 默认10个, Number of max buffers for each output subparition 每个Channel最多Buffer数量; 
        int maxBuffersPerChannel = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_MAX_BUFFERS_PER_CHANNEL);
		// sort-shuffle.min-buffers, 默认64, Minimum number required per sort-merge blocking result partition; 
        int sortShuffleMinBuffers = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        // sort-shuffle.min-parallelism, 默认int.MaxValue ;  small parallelism hash-based , large parallelism  sort-merge
		int sortShuffleMinParallelism = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM);
		// detailed-metrics, 细节监控 
        boolean isNetworkDetailedMetrics = configuration.getBoolean(NettyShuffleEnvironmentOptions.NETWORK_DETAILED_METRICS);

        String[] tempDirs = ConfigurationUtils.parseTempDirectories(configuration);
		// exclusive-buffers-request-timeout-ms, 默认30s, The timeout for requesting exclusive buffers for each channel
        Duration requestSegmentsTimeout =Duration.ofMillis(configuration.getLong( NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));

        BoundedBlockingSubpartitionType blockingSubpartitionType = getBlockingSubpartitionType(configuration);
		// blocking-shuffle.compression.enabled, 默认false, 不压缩 
        boolean blockingShuffleCompressionEnabled =configuration.get(NettyShuffleEnvironmentOptions.BLOCKING_SHUFFLE_COMPRESSION_ENABLED);
        String compressionCodec = configuration.getString(NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC);

        return new NettyShuffleEnvironmentConfiguration();

	}
	
	return createNettyShuffleEnvironment(networkConfig);{
		
	}
}







// LocalBufferPool 容量配置逻辑
Task.doRun(){
	setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);{
		// 正常1个输出, 对应1个ResultPartitionWriter, 可能有多个结果分区 
		for (ResultPartitionWriter partition : producedPartitions) {
			partition.setup();{
				BufferPool bufferPool = checkNotNull(bufferPoolFactory.apply(this));{
					// 
					/* 默认PIPELINED_BOUNDED模式, 最大 maxSegment = ( numberOfSubpartitions * networkBuffersPerChannel ) + floatingNetworkBuffersPerGate 
					* 		maxSegment = ( 并行度 * 单channel缓存数(默认2) ) +  浮动缓存数(默认 8) 
					* 		maxSegment = (parallelism * memory.buffers-per-channel ) + floating-buffers-per-gate 
					*/
					int maxNumberOfMemorySegments = type.isBounded() ? 
						numberOfSubpartitions * networkBuffersPerChannel + floatingNetworkBuffersPerGate : 
						Integer.MAX_VALUE;
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


TM进程里, 接受并启动1个Task的运行; 

TaskExecutor.submitTask(){
	Task task = new Task();{
		
		// produced intermediate result partitions
        final ResultPartitionWriter[] resultPartitionWriters = shuffleEnvironment.createResultPartitionWriters(taskShuffleContext, resultPartitionDeploymentDescriptors).toArray(new ResultPartitionWriter[] {});
		
		// consumed intermediate result partitions
		IndexedInputGate[] gates =shuffleEnvironment.createInputGates(taskShuffleContext, this, inputGateDeploymentDescriptors).toArray(new IndexedInputGate[0]);{
			InputChannelMetrics inputChannelMetrics = new InputChannelMetrics(networkInputGroup, ownerContext.getParentGroup());
			SingleInputGate[] inputGates = new SingleInputGate[inputGateDeploymentDescriptors.size()];
			for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
				InputGateDeploymentDescriptor igdd =inputGateDeploymentDescriptors.get(gateIndex);
				SingleInputGate inputGate = singleInputGateFactory.create(ownerContext.getOwnerName(),partitionProducerStateProvider,inputChannelMetrics);{
					SupplierWithException<BufferPool, IOException> bufferPoolFactory =createBufferPoolFactory(networkBufferPool,networkBuffersPerChannel,);
					
					SingleInputGate inputGate = new SingleInputGate()
					
					
					createInputChannels(owningTaskName, igdd, inputGate, metrics);{//org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory
						//1个 (task的) inputGate 有多个 InputChannel, 每个inputChannel对应上游Subtask的1个并行度;
						// ShuffleDescriptor 代表1个shuffle-channel的信息, 其变量 producerLocation, producerId 表示发送方的JVM实例和 subtask_Id; 
						InputChannel[] inputChannels = new InputChannel[shuffleDescriptors.length];
						
						for (int i = 0; i < inputChannels.length; i++) {
							inputChannels[i] =createInputChannel(inputGate, i, shuffleDescriptors[i], channelStatistics, metrics); {
								return applyWithShuffleTypeCheck();{
									createKnownInputChannel();{
										ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
										// 两个 resourceId相同,则为同一Resource 
										boolean isLoal = inputChannelDescriptor.isLocalTo(taskExecutorResourceId);{
											return producerLocation.equals(consumerLocation);
										}
										if (isLoal) {
											channelStatistics.numLocalChannels++;
											return new LocalRecoveredInputChannel(
												inputGate,
												index, partitionId,
												partitionManager, taskEventPublisher,
												partitionRequestInitialBackoff,
												partitionRequestMaxBackoff, networkBuffersPerChannel, metrics);{
												// 父类构造: new RecoveredInputChannel()
												super();{
													super();
													bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);
													this.networkBuffersPerChannel = networkBuffersPerChannel;
												}
												this.partitionManager = checkNotNull(partitionManager);
												this.taskEventPublisher = checkNotNull(taskEventPublisher);
											}
										} else {
											return new RemoteRecoveredInputChannel();
										}
									}
								}
							}
						}
					}
					return inputGate;
				}
			}
		}
		
	}
	
	taskAdded = taskSlotTable.addTask(task);
	task.startTaskThread();
	
}


// flink1.12x: Network Configuration 
参数含义分析: buffersPerChannel	
buffersPerChannel

RecoveredInputChannel extends InputChannel{
	ArrayDeque<Buffer> receivedBuffers = new ArrayDeque<>();
	BufferManager bufferManager;
	int networkBuffersPerChannel;
}
* 包括2个子类: LocalRecoveredInputChannel, RemoteRecoveredInputChannel



maxNumberOfMemorySegments	 总Segment数据 
	- maxSegment = ( numberOfSubpartitions * networkBuffersPerChannel ) + floatingNetworkBuffersPerGate 
	- maxSegment = ( parallelism * memory.buffers-per-channel(默认2) ) + floating-buffers-per-gate(默认8) 


maxBuffersPerChannel	默认10个, 单个 targetChannel 给的数量太多(超过10个), 就不给MemorySegment; 
	- 默认10个, 可有max-buffers-per-channel 指定; 
	
class LocalBufferPool implements BufferPool {
	final NetworkBufferPool networkBufferPool;
	// The minimum number of required segments for this pool.
	final int numberOfRequiredMemorySegments;
	// 每个 
	ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<MemorySegment>();
	//Maximum number of network buffers to allocate.
	final int maxNumberOfMemorySegments;
	// 每个Channel 最多给的MemSegment/buffer数量,超过10个就不给; 
	final int maxBuffersPerChannel;
	
	// 
	MemorySegment requestMemorySegment(int targetChannel) {
		// target channel over quota; do not return a segment
		if (subpartitionBuffersCount[targetChannel] >= maxBuffersPerChannel) {
			return null;
		}
		segment = availableMemorySegments.poll();
		if (++subpartitionBuffersCount[targetChannel] == maxBuffersPerChannel) {
			unavailableSubpartitionsCount++;
        }
		return segment;
	}
	
}



// flink_network_metrics 监控指标

METRIC_INPUT_POOL_USAGE

class CreditBasedInputBuffersUsageGauge extends AbstractBuffersUsageGauge  {
	
	public Float getValue() {
        int usedBuffers = 0;
        int totalBuffers = 0;
        for (SingleInputGate inputGate : inputGates) {
			// 算出每个 inputGate的:  numberOfRequestedMemorySegments - availableMemorySegments.size() + sum( bufferQueue.floatingBuffers.size()) 
            usedBuffers += calculateUsedBuffers(inputGate);
			// 算出每个 inputGate的:
            totalBuffers += calculateTotalBuffers(inputGate);
        }
		
        if (totalBuffers != 0) {
            return ((float) usedBuffers) / totalBuffers;
        } else {
            return 0.0f;
        }
		
	}
		
	
	public int calculateUsedBuffers(SingleInputGate inputGate) {
		int floatingBuffersUsage = floatingBuffersUsageGauge.calculateUsedBuffers(inputGate);{// FloatingBuffersUsageGauge.
			int availableFloatingBuffers = 0;
			BufferPool bufferPool = inputGate.getBufferPool();
			if (bufferPool != null) {
				// 最大 - 可用的buffer 
				int requestedFloatingBuffers = bufferPool.bestEffortGetNumOfUsedBuffers();{// LocalBufferPool
					return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
				}
				for (InputChannel ic : inputGate.getInputChannels().values()) {
					if (ic instanceof RemoteInputChannel) {
						availableFloatingBuffers += ((RemoteInputChannel) ic).unsynchronizedGetFloatingBuffersAvailable();
					}
				}
				return Math.max(0, requestedFloatingBuffers - availableFloatingBuffers);
			}
			return 0;
		}
		exclusiveBuffersUsage = exclusiveBuffersUsageGauge.calculateUsedBuffers(inputGate); {// ExclusiveBuffersUsageGauge
			int usedBuffers = 0;
			for (InputChannel ic : inputGate.getInputChannels().values()) {
				if (ic instanceof RemoteInputChannel) {
					usedBuffers += ((RemoteInputChannel) ic).unsynchronizedGetExclusiveBuffersUsed();
				}
			}
			return usedBuffers;
		}
		
		return floatingBuffersUsage + exclusiveBuffersUsage;
	}
	
	
	@Override
    public int calculateTotalBuffers(SingleInputGate inputGate) {
		// LocalBufferPool.currentPoolSize  
		floatingBuffersUsage = floatingBuffersUsageGauge.calculateTotalBuffers(inputGate);{
			BufferPool bufferPool = inputGate.getBufferPool();
			if (bufferPool != null) {
				return inputGate.getBufferPool().getNumBuffers();
			}
			return 0;
		}
		
		exclusiveBuffersUsage = exclusiveBuffersUsageGauge.calculateTotalBuffers(inputGate);{
			int totalExclusiveBuffers = 0;
			for (InputChannel ic : inputGate.getInputChannels().values()) {
				if (ic instanceof RemoteInputChannel) {
					totalExclusiveBuffers += ((RemoteInputChannel) ic).getInitialCredit();
				}
			}
			return totalExclusiveBuffers;
		}
        return floatingBuffersUsage  + exclusiveBuffersUsage;
    }
	
}



InputBuffersGauge {
    public Integer getValue() {
        int totalBuffers = 0;
        for (SingleInputGate inputGate : inputGates) {
            totalBuffers += inputGate.getNumberOfQueuedBuffers();{
                int totalBuffers = 0;
                for (InputChannel channel : inputChannels.values()) {
                    totalBuffers += channel.unsynchronizedGetNumberOfQueuedBuffers();
                }
                return totalBuffers;
			}
        }

        return totalBuffers;
    }
}