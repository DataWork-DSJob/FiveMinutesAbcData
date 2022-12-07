


//flink-core_state_src1.12x: 状态存储的初始化 

StreamTask.doRun() -> invoke() {// StreamTask.invoke()
	 // 真正消费数据前,先进行初始化
	beforeInvoke();{
		actionExecutor.runThrowing(()-> {
			SequentialChannelStateReader reader = getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
			
			operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());{//OperatorChain.initializeStateAndOpenOperators(streamTaskStateInitializer)
				for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
					StreamOperator<?> operator = operatorWrapper.getStreamOperator();
					// 这里初始化
					operator.initializeState(streamTaskStateInitializer);{
						final TypeSerializer<?> keySerializer =config.getStateKeySerializer(getUserCodeClassloader());
						
						final StreamOperatorStateContext context = streamTaskStateManager.streamOperatorStateContext(
								getOperatorID(),getClass().getSimpleName(),
								getProcessingTimeService(), this,
								keySerializer,
								streamTaskCloseableRegistry,metrics,
								config.getManagedMemoryFractionOperatorUseCaseOfSlot(
										ManagedMemoryUseCase.STATE_BACKEND,
										runtimeContext.getTaskManagerRuntimeInfo().getConfiguration(),
										runtimeContext.getUserCodeClassLoader()),
								isUsingCustomRawKeyedState());{// StreamTaskStateInitializerImpl.streamOperatorStateContext()
							
							// -------------- Keyed State Backend --------------
							keyedStatedBackend = keyedStatedBackend(keySerializer,operatorIdentifierText,);{
								
								FunctionWithException instanceSupplier = () -> stateBackend.createKeyedStateBackend();
								BackendRestorerProcedure backendRestorer = new BackendRestorerProcedure<>(instanceSupplier);
								
								return backendRestorer.createAndRestore( prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState());{
									while (alternativeIdx < restoreOptions.size()) {
										Collection<S> restoreState = restoreOptions.get(alternativeIdx);
										return attemptCreateAndRestore(restoreState); {
											final T backendInstance = instanceSupplier.apply(restoreState); {
												stateBackend.createKeyedStateBackend(){// 
													// RocksDB 存储的实现, 源码细节参见下面; 
													RocksDBStateBackend.createKeyedStateBackend();
													
													// Fs
													FsStateBackend.createKeyedStateBackend();
													
													
												}
											}
											return backendInstance;
										}
									}
								}
							}
										
							// -------------- Raw State Streams --------------
							rawKeyedStateInputs =rawKeyedStateInputs(prioritizedOperatorSubtaskStates.getPrioritizedRawKeyedState().iterator());
							streamTaskCloseableRegistry.registerCloseable(rawKeyedStateInputs);
							rawOperatorStateInputs =rawOperatorStateInputs();
							
							timeServiceManager = timeServiceManagerProvider.create();
							 return new StreamOperatorStateContextImpl();
			
						}
						
					}
					 /**
					*  就是Rich算子的open()方法
					*/
					operator.open();{ // StatefulFunction.open()
						StreamFilter.open();
						
						// jdbc sink算子的 open初始化源码; 
						SinkOperator.open(){
							super.open();{//AbstractUdfStreamOperator.open()
								super.open();
								FunctionUtils.openFunction(userFunction, new Configuration());{
									if (function instanceof RichFunction) {
										richFunction.open(parameters);{
											// ????Jdbc????, ????Open ???; ?????? 3.2.1 
											GenericJdbcSinkFunction.open(){
												super.open(parameters);
												RuntimeContext ctx = getRuntimeContext();
												outputFormat.setRuntimeContext(ctx);
												outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());{//JdbcOutputFormat.open()
													connectionProvider.getOrEstablishConnection();
													jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
													this.scheduledFuture =this.scheduler.scheduleWithFixedDelay(()->flush(),);
												}
											}
										}
									}
								}
							}
							this.sinkContext = new SimpleContext(getProcessingTimeService());
						}
						// KeyBy, Window算子, 这里会按key 加载状态; 
						KeyedProcessOperator.open(){
							
						}
						
						StreamExecCalc$21.open(){
							
						}
					}
					
				}
			}
		});
	}
                
	// 在这里里面循环接受消息,并运行;
	runMailboxLoop();
	
	// 结束消费和处理后,资源释放;
	afterInvoke();
	
}



frocksdbjni-5.17.2-artisans-2.0 包中, 有如下几个jni lib包:
	librocksdbjni-linux32.so
	librocksdbjni-linux64.so
	librocksdbjni-linux-ppc64le.so
	librocksdbjni-osx.jnilib
	librocksdbjni-win64.dll
	
	
	// 
	
	// 
	
	


Environment.getJniLibraryName(){
	// window系统: windows 10
	// 
	String OS = System.getProperty("os.name").toLowerCase();
	
    if (isUnix()) { // return OS.contains("nix") || OS.contains("nux");
      final String arch = is64Bit() ? "64" : "32";
      if(isPowerPC()) {
        return String.format("%sjni-linux-%s", name, ARCH);
      } else if(isS390x()) {
        return String.format("%sjni-linux%s", name, ARCH);
      } else {
        return String.format("%sjni-linux%s", name, arch);
      }
    } else if (isMac()) { 		// return (OS.contains("mac"));
      return String.format("%sjni-osx", name);
    } else if (isFreeBSD()) { 	// return (OS.contains("freebsd"));
      return String.format("%sjni-freebsd%s", name, is64Bit() ? "64" : "32");
    } else if (isAix() && is64Bit()) {
      return String.format("%sjni-aix64", name);
    } else if (isSolaris()) {	//return OS.contains("sunos");
      final String arch = is64Bit() ? "64" : "32";
      return String.format("%sjni-solaris%s", name, arch);
	  
    } else if (isWindows() && is64Bit()) {	// return (OS.contains("win"));
		Environment.is64Bit(){
			if (ARCH.indexOf("sparcv9") >= 0) { // ARCH: window:amd64, 
			  return true;
			}
			return (ARCH.indexOf("64") > 0);
		}
      // 结果就返回 rocksdbjni-win64 (jar根目录下的dll文件)
	  return String.format("%sjni-win64", name);
    } else if (isOpenBSD()) {
      return String.format("%sjni-openbsd%s", name, is64Bit() ? "64" : "32");
    }
    throw new UnsupportedOperationException(String.format("Cannot determine JNI library name for ARCH='%s' OS='%s' name='%s'", ARCH, OS, name));
}


RocksDBStateBackend.createKeyedStateBackend(){
	// Window系统: C:\Users\ALLENH~1\AppData\Local\Temp\
	// Linux: 
	String tempDir = env.getTaskManagerInfo().getTmpDirectories()[0];
	// 这里加载RocksDB的lib, 否则可能会报 UnsatisfiedLinkError 错误;
	ensureRocksDBIsLoaded(tempDir);{//RocksDBStateBackend.ensureRocksDBIsLoaded
		File tempDirParent = new File(tempDirectory).getAbsoluteFile();
		// 默认重试3次;
		for (int attempt = 1; attempt <= ROCKSDB_LIB_LOADING_ATTEMPTS; attempt++) {
			// C:\Users\ALLENH~1\AppData\Local\Temp\rocksdb-lib-c465974af2ca3cb9298590f20130fd6f\librocksdbjni-win64.dll
			rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + new AbstractID());
			LOG.debug("Attempting to create RocksDB native library folder {}", rocksLibFolder);
			rocksLibFolder.mkdirs();
			// explicitly load the JNI dependency if it has not been loaded before
            NativeLibraryLoader.getInstance().loadLibrary(rocksLibFolder.getAbsolutePath());
			// this initialization here should validate that the loading succeeded
            // Loads the necessary library files. extracts the shared library for loading at java.io.tmpdir, 
			// however, you can override this temporary location by setting the environment variable ROCKSDB_SHAREDLIB_DIR.
			
			RocksDB.loadLibrary();{
				if (libraryLoaded.get() == LibraryState.LOADED) {
				  return;
				}
				if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED, LibraryState.LOADING)) {
					final String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
					for (final CompressionType compressionType : CompressionType.values()) {
						if (compressionType.getLibraryName() != null) {
							System.loadLibrary(compressionType.getLibraryName());
						}
					}
					NativeLibraryLoader.getInstance().loadLibrary(tmpDir);{//NativeLibraryLoader.loadLibrary(tmpDir)
						try {
							// sharedLibraryName = Environment.getSharedLibraryName("rocksdb"); 默认值是"rocksdbjni"
							// 加载 java.class.path中名为 rocksdbjni.dll 文件;
							System.loadLibrary(sharedLibraryName);
						} catch(final UnsatisfiedLinkError ule1) {//Window系统会报这个错误, 进入这里; 
						  try {
							// jniLibraryName = Environment.getJniLibraryName("rocksdb");默认值是"rocksdbjni-win64"
							// 从java.class.path中名为 rocksdbjni-win64.dll 文件;
							System.loadLibrary(jniLibraryName);
						  } catch(final UnsatisfiedLinkError ule2) {
							loadLibraryFromJar(tmpDir);{
								// 
								File jarFilePath = loadLibraryFromJarToTemp(tmpDir);{
									if (tmpDir == null || tmpDir.isEmpty()) {
									  temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
									} else {// 一般上面创建创建了临时目录, 进入这里; 
									  // jniLibraryFileName = Environment.getJniLibraryFileName("rocksdb"); window环境默认是 "librocksdbjni-win64.dll"
									  // 即在临时目录下,新建 librocksdbjni-win64.dll文件; 
									  // C:\Users\ALLENH~1\AppData\Local\Temp\rocksdb-lib-f4dc1ddc3483707024abc68571f678f5\librocksdbjni-win64.dll
									  temp = new File(tmpDir, jniLibraryFileName);
									  if (temp.exists() && !temp.delete()) {
										throw new RuntimeException("File: " + temp.getAbsolutePath() + " already exists and cannot be removed.");
									  }
									  // 新建该文件; 
									  if (!temp.createNewFile()) {
										throw new RuntimeException("File: " + temp.getAbsolutePath() + " could not be created.");
									  }
									}
									
									if (!temp.exists()) {
									  throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
									} else {//作为临时文件, 设置退出删除;
									  temp.deleteOnExit();
									}
									// attempt to copy the library from the Jar file to the temp destination
									//用ClassLoader加载 $jniLibraryFileName 文件 (linux: librocksdbjni-win64.dll, window: librocksdbjni-win64.dll)
									final InputStream is = getClass().getClassLoader().getResourceAsStream(jniLibraryFileName));
									if (is == null) {
										throw new RuntimeException(jniLibraryFileName + " was not found inside JAR.");
									} else {
										// 把 frocksdbjni-5.17.2.jar中的 相应lib包(包括linux,window等5个版本) 拷贝到临时文件中; 
										// jniLibraryFileName 在这个目录: jar:file:/D:/repo/mvnRepo/com/data-artisans/frocksdbjni/5.17.2-artisans-2.0/frocksdbjni-5.17.2-artisans-2.0.jar!/librocksdbjni-win64.dll
										Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
									}
									return temp;
									
								}
								// 把第三方本地包(绝对路径) 加载到JVM中
								System.load(jarFilePath.getAbsolutePath());{
									Runtime.getRuntime().load0(Reflection.getCallerClass(), filename);
								}
							}
						  }
						}
					}
					libraryLoaded.set(LibraryState.LOADED);
					return;
				} 
				// 要变成Load才能继续后面的计算;
				while (libraryLoaded.get() == LibraryState.LOADING) {
					Thread.sleep(10);
				}
			}
			
		}
	}
	
	
	lazyInitializeForJob(env, fileCompatibleIdentifier);
	// C:\Users\ALLENH~1\AppData\Local\Temp\flink-io-5eb25019-55d1-4307-9091-46dd77f753e0\job_d67b4966d5ad4b53ab065f5d1ca944bd_op_WindowOperator_5e4132aa89d205854be108b7fd747a9c__2_2__uuid_1f80074b-ee98-4fd6-9aee-aee0c29e987b
	File instanceBasePath = new File(getNextStoragePath(), "job_"+ jobId+ "_op_"+ fileCompatibleIdentifier+ "_uuid_"+ UUID.randomUUID());
	
	OpaqueMemoryResource<RocksDBSharedResources> sharedResources = RocksDBOperationUtils.allocateSharedCachesIfConfigured(memoryConfiguration,
					env.getMemoryManager(), managedMemoryFraction, LOG);{
		final double highPriorityPoolRatio = memoryConfig.getHighPriorityPoolRatio();
        final double writeBufferRatio = memoryConfig.getWriteBufferRatio();

        final LongFunctionWithException<RocksDBSharedResources, Exception> allocator = (size) -> RocksDBMemoryControllerUtils.allocateRocksDBSharedResources(
                                size, writeBufferRatio, highPriorityPoolRatio);
		// 如果是固定Memory 
		if (memoryConfig.isUsingFixedMemoryPerSlot()) {
			
		} else {
			// 每个slot不是贡献内存,而是共享内存, 则要计算机分配了?
			
			return memoryManager.getSharedMemoryResourceForManagedMemory(
						MANAGED_MEMORY_RESOURCE_ID, // state-rocks-managed-memory
						allocator, memoryFraction);{//org.apache.flink.runtime.memory.MemoryManager
				// if we need to allocate the resource (no shared resource allocated, yet), this would be
				// fractionToInitializeWith 默认1, 即全部分配; 
				final long numBytes = computeMemorySize(fractionToInitializeWith);{
					validateFraction(fraction);
					return (long) Math.floor(memoryBudget.getTotalMemorySize() * fraction);
				}
				
				// 这里定义 initializer 方法,下面createResource()中调用 
				LongFunctionWithException<T, Exception> reserveAndInitialize = (size) -> { 
					reserveMemory(type, size);
					return initializer.apply(size);
				};
				
				SharedResources.ResourceAndSize<T> resource = sharedResources.getOrAllocateSharedResource(type, leaseHolder, reserveAndInitialize, numBytes);{//SharedResources
					LeasedResource<T> resource = (LeasedResource<T>) reservedResources.get(type);
					if (resource == null) {
						resource = createResource(initializer, sizeForInitialization); {//SharedResources
							final T resource = initializer.apply(size);{ //
								// 上面 LongFunctionWithException<T, Exception> reserveAndInitialize  变量
								reserveMemory(type, size);
								return initializer.apply(size);{
									RocksDBMemoryControllerUtils.allocateRocksDBSharedResources( size, writeBufferRatio, highPriorityPoolRatio);{
										// 算出缓存容量: 55924053, 
										/** 算出 RocksDB的 cache大小, 默认总大小(64M)的 六分之五; 
										*	totalMemorySize	= 67108864 = 64M
										*	writeBufferRatio = 0.5
										*	cache = (3-0.5) * 64M / 3 = 64 * 2.5 / 3 = 64M * 0.8333 = 53.33M
										*		= 55924053
										*	cache = 64M 的六分之五;
										*/
										// highPriorityPoolRatio = 0.1, 
										long calculatedCacheCapacity = RocksDBMemoryControllerUtils.calculateActualCacheCapacity(totalMemorySize, writeBufferRatio);{
											return (long) ((3 - writeBufferRatio) * totalMemorySize / 3);
										}
										final Cache cache =RocksDBMemoryControllerUtils.createCache(calculatedCacheCapacity, highPriorityPoolRatio);{
											return new LRUCache(cacheCapacity, -1, false, highPriorityPoolRatio);
										}
										/** buff大小: 总状态内存三分之二的一般, 即默认 三分之一;  
										
										*  totalMemorySize, 状态内存默认64M, 		writeBufferRatio,装写buff比例 默认 0.5 ;
										* 	writeBufferManagerCapacity = totalMemorySize * (2/3) * writeBufferRatio 
										*		= 64M * 0.667 * 0.5 = 21.33M
										* 		
										*/
										long writeBufferManagerCapacity = RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity(totalMemorySize, writeBufferRatio);{
											return (long) (2 * totalMemorySize * writeBufferRatio / 3);
										}
										// 总内存: 64M, 	writeBuffer用三分之一: 21M,			cache缓存,用六分之五: 53M;
										final WriteBufferManager wbm =RocksDBMemoryControllerUtils.createWriteBufferManager(writeBufferManagerCapacity, cache);
										return new RocksDBSharedResources(cache, wbm, writeBufferManagerCapacity);
									}
								}
							}
							return new LeasedResource<>(resource, size);
						}
						reservedResources.put(type, resource);
					}
					return resource;
				}
				final long size = resource.size();
				return new OpaqueMemoryResource<>(resource.resourceHandle(), size, disposer);
			}
		}
		
	}
	
}













