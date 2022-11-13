
// 1.0 
bin/flink run �������Ự����


// 1.1 bin/flink  �ű�Դ��
bin=`dirname "$target"`
# ���ػ�������
. "$bin"/config.sh

CC_CLASSPATH=`constructFlinkClassPath`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-client-$HOSTNAME.log
# ����JVM����
FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_CLI}"

exec $JAVA_RUN $JVM_ARGS $FLINK_ENV_JAVA_OPTS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.client.cli.CliFrontend "$@"

	// java -cp "xx.jar"  org.apache.flink.client.cli.CliFrontend "$@"
	// java -cp "xx.jar"  org.apache.flink.client.cli.CliFrontend run -d -e kubernetes-session xx



// 1.2 KubernetesSessionCli.main() ��Java�ύ����

org.apache.flink.client.cli.CliFrontend.main(){
	EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);
	final String configurationDirectory = getConfigurationDirectoryFromEnv();
	final Configuration configuration =GlobalConfiguration.loadConfiguration(configurationDirectory);
	final List<CustomCommandLine> customCommandLines =loadCustomCommandLines(configuration, configurationDirectory);
	final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
	int retCode =SecurityUtils.getInstalledContext().runSecured(() -> cli.parseAndRun(args));{// CliFrontend.parseAndRun
		String action = args[0];
		final String[] params = Arrays.copyOfRange(args, 1, args.length);
		try {
            // do action
            switch (action) {
                case ACTION_RUN:
                    run(params);{
						final Options commandOptions = CliFrontendParser.getRunCommandOptions();
						final CommandLine commandLine = getCommandLine(commandOptions, args, true);
						final List<URL> jobJars = getJobJarAndDependencies(programOptions);
						final Configuration effectiveConfiguration =getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);
						
						executeProgram(effectiveConfiguration, program);{
							ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);{
								final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
								ContextEnvironment.setAsContext(executorServiceLoader,suppressSysout);
								StreamContextEnvironment.setAsContext(executorServiceLoader,suppressSysout);
								
								program.invokeInteractiveModeForExecution();{
									callMainMethod(mainClass, args);{
										// �� driver/app�� �����ȡ�� main()����,����ִ��
										mainMethod = entryClass.getMethod("main", String[].class);
										// ִ���� main()����:examples.join.WindowJoin.main() ���û��Զ���� UdfDriver.main()
										mainMethod.invoke(null, (Object) args);
										
									}
								}
							
							}
						}
						
					}
                    return 0;
                case ACTION_RUN_APPLICATION:
                    runApplication(params);
                    return 0;
                case ACTION_LIST:
                    list(params);
                    return 0;
                case ACTION_INFO:
                    info(params);
                    return 0;
                case ACTION_CANCEL:
                    cancel(params);
                    return 0;
                case ACTION_STOP:
                    stop(params);
                    return 0;
                case ACTION_SAVEPOINT:
                    savepoint(params);
                    return 0;
                case "-h":
                case "--help":
                    CliFrontendParser.printHelp(customCommandLines);
                    return 0;
                case "-v":
                case "--version":
                    
                    return 0;
                default:
                    
                    return 1;
            }
        } catch (CliArgsException ce) {
            return handleArgException(ce);
        } catch (ProgramParametrizationException ppe) {
            return handleParametrizationException(ppe);
        } catch (ProgramMissingJobException pmje) {
            return handleMissingJobException();
        } catch (Exception e) {
            return handleError(e);
        }
		
	}
	System.exit(retCode);
	
}



org.apache.flink.streaming.examples.join.WindowJoin.main(){
	final ParameterTool params = ParameterTool.fromArgs(args);
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	env.getConfig().setGlobalJobParameters(params);
	// �����߼�
	DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowJoin(grades, salaries, windowSize);
	
	// �ύִ��
	env.execute("Windowed Join Example");{
		StreamGraph streamGraph = getStreamGraph(jobName);{
			StreamGraph streamGraph = getStreamGraphGenerator().setJobName(jobName).generate();
			return streamGraph;
		}
		return execute(streamGraph);{
			// ��ȡ JobMaster�ĵ�����client,���������
			final JobClient jobClient = executeAsync(streamGraph);
			
		}
	}
	
}


StreamExecutionEnvironment.execute(){
	return execute(getStreamGraph(jobName));{
		// �ֱ���ִ�л����� Զ��ִ�л���
		LocalStreamEnvironment.execute(){
			return super.execute(streamGraph);{//StreamExecutionEnvironment.execute()
				final JobClient jobClient = executeAsync(streamGraph);{
					// ��CL�м��ؽ������е� PipelineExecutorFactory ʵ����,ֻ��flink-clients��LocalExecutorFactory, RemoteExecutorFactory 2����
					final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);{//core.DefaultExecutorServiceLoader.
						final ServiceLoader<PipelineExecutorFactory> loader = ServiceLoader.load(PipelineExecutorFactory.class);
						while (loader.iterator().hasNext()) {
							final PipelineExecutorFactory factory = factories.next();
							if (factory != null && factory.isCompatibleWith(configuration)){
								// configuration�е�executor.target == kubernetes-session
								KubernetesSessionClusterExecutorFactory.isCompatibleWith(){
									return configuration
									.get(DeploymentOptions.TARGET)
									.equalsIgnoreCase(KubernetesSessionClusterExecutor.NAME);
								}
								// configuration��executor.target == yarn-per-job
								YarnJobClusterExecutorFactory.isCompatibleWith(){
									return YarnJobClusterExecutor.NAME.equalsIgnoreCase(
												configuration.get(DeploymentOptions.TARGET));
								}
								
								// configuration��executor.target == yarn-session
								
								
							} {
								compatibleFactories.add(factories.next());
							}
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
						.getExecutor(configuration) {// PipelineExecutor.getExecutor()
							// K8s Sessionģʽ
							KubernetesSessionClusterExecutorFactory.getExecutor(){
								return new KubernetesSessionClusterExecutor();
							}
							
							// Yarn Per Jobģʽ
							YarnJobClusterExecutorFactory.getExecutor(){
								return new YarnJobClusterExecutor();
							}
							
							
						}
                        .execute(streamGraph, configuration, userClassloader);{
							
							KubernetesSessionClusterExecutor.execute(){
								AbstractSessionClusterExecutor.execute();{
									final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);{
										
									}
									final ClusterDescriptor<ClusterID> clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);{
										
										KubernetesSessionClusterExecutorFactory.createClusterDescriptor(){
											if (!configuration.contains(KubernetesConfigOptions.CLUSTER_ID)) {
												final String clusterId = generateClusterId();
												configuration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId); //kubernetes.cluster-id
											}
											FlinkKubeClient flinkKubeClient = FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "client");{//FlinkKubeClientFactory.fromConfiguration()
												
												return new Fabric8FlinkKubeClient(flinkConfig, client, createThreadPoolForAsyncIO(poolSize, useCase));
											}
											return new KubernetesClusterDescriptor( configuration,flinkKubeClient);
										}
										
										YarnJobClusterExecutorFactory.createClusterDescriptor()
										
									}
									
									final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
									
									final ClusterClientProvider<ClusterID> clusterClientProvider =clusterDescriptor.retrieve(clusterID);{//
										final ClusterClientProvider<String> clusterClientProvider =createClusterClientProvider(clusterId);
										ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient();{
											final Configuration configuration = new Configuration(flinkConfig);
											final Optional<Endpoint> restEndpoint = client.getRestEndpoint(clusterId);{//Fabric8FlinkKubeClient.getRestEndpoint()
												final Service service = restService.get().getInternalResource();
												// ��ȡ restService,��ȡ���е� spec.ports.0.port=8081 ��Ϊ restPort; 
												final int restPort = getRestPortFromExternalService(service);
												final KubernetesConfigOptions.ServiceExposedType serviceExposedType =KubernetesConfigOptions.ServiceExposedType.valueOf(service.getSpec().getType());
												return getRestEndPointFromService(service, restPort);
											}
											return new RestClusterClient<>(configuration,clusterId,new StandaloneClientHAServices(getWebMonitorAddress(configuration)));
										}
										return clusterClientProvider;
										
									}
									
									// �첽�ύ�͵ȴ����
									return clusterClient
											.submitJob(jobGraph)
											.thenApplyAsync()
											.thenApplyAsync()
											.whenCompleteAsync((ignored1, ignored2) -> clusterClient.close());
									
									
								}
							}
							
							LocalExecutor.execute()
							
							AbstractJobClusterExecutor.execute();
							
							AbstractSessionClusterExecutor.execute();
							
							RemoteExecutor[extends AbstractSessionClusterExecutor].execute();
							
							EmbeddedExecutor.execute();
						}
						
					JobClient jobClient = jobClientFuture.get();
					jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
					return jobClient;
				}
				jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));
				return jobExecutionResult;
			}
		}
		
		RemoteStreamEnvironment.execute(){
			
		}
		
		StreamContextEnvironment.execute();
		
		StreamPlanEnvironment.execute();// ? strema sql ?
		
	}
}


// K8s Sesion��

KubernetesSessionClusterExecutorFactory.createClusterDescriptor(){
	if (!configuration.contains(KubernetesConfigOptions.CLUSTER_ID)) {
		final String clusterId = generateClusterId();
		configuration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId); //kubernetes.cluster-id
	}
	FlinkKubeClient flinkKubeClient = FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "client");{//FlinkKubeClientFactory.fromConfiguration()
		final String kubeContext = flinkConfig.getString(KubernetesConfigOptions.CONTEXT);
		// ��鿴�Ƿ���� k8s�����ļ� kubernetes.config.file
		final String kubeConfigFile =flinkConfig.getString(KubernetesConfigOptions.KUBE_CONFIG_FILE);
		if (kubeConfigFile != null) {// ���� kubernetes.config��flink�����ļ�;
			config =Config.fromKubeconfig( kubeContext, FileUtils.readFileUtf8(new File(kubeConfigFile)),null);
		}else{ // ʹ��Ĭ��k8s����
			config = Config.autoConfigure(kubeContext);{
				Config config = new Config();
				return autoConfigure(config, context);{
					// �����������;
					if (!tryKubeConfig(config, context)) {{//Config.tryKubeConfig()
						if (Utils.getSystemPropertyOrEnvVar(KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, true)){
							// ��/home/bigdata/.kube/config Ŀ¼��ȡ����
							String fileName = Utils.getSystemPropertyOrEnvVar(KUBERNETES_KUBECONFIG_FILE, new File(getHomeDir(), ".kube" + File.separator + "config").toString());
							FileReader reader = new FileReader(kubeConfigFile);
							kubeconfigContents = IOHelpers.readFully(reader); // ���� .kube/config �������ı��ļ�����; 
							Config.loadFromKubeconfig(config, context, kubeconfigContents, kubeConfigFile.getPath());{
								io.fabric8.kubernetes.api.model.Config kubeConfig = KubeConfigUtils.parseConfigFromString(kubeconfigContents);
								Context currentContext = KubeConfigUtils.getCurrentContext(kubeConfig);
								if (currentCluster != null) {
									config.setMasterUrl(currentCluster.getServer());
									config.setNamespace(currentContext.getNamespace());
									config.setTrustCerts(currentCluster.getInsecureSkipTlsVerify() != null && currentCluster.getInsecureSkipTlsVerify());
								}
								
							}
							return true;
						}
					}
					  tryServiceAccount(config);
					  tryNamespaceFromPath(config);
					}
					// ��ϵͳ������, overwide��д���� .kube/config������; 
					configFromSysPropsOrEnvVars(config);{
						config.setTrustCerts(Utils.getSystemPropertyOrEnvVar(KUBERNETES_TRUST_CERT_SYSTEM_PROPERTY, config.isTrustCerts()));
						config.setDisableHostnameVerification(Utils.getSystemPropertyOrEnvVar(KUBERNETES_DISABLE_HOSTNAME_VERIFICATION_SYSTEM_PROPERTY, config.isDisableHostnameVerification()));
						// ��ͨ�� kubernetes.master ��дmaster
						config.setMasterUrl(Utils.getSystemPropertyOrEnvVar(KUBERNETES_MASTER_SYSTEM_PROPERTY, config.getMasterUrl()));
						config.setApiVersion(Utils.getSystemPropertyOrEnvVar(KUBERNETES_API_VERSION_SYSTEM_PROPERTY, config.getApiVersion()));
						config.setNamespace(Utils.getSystemPropertyOrEnvVar(KUBERNETES_NAMESPACE_SYSTEM_PROPERTY, config.getNamespace()));
						config.setCaCertFile(Utils.getSystemPropertyOrEnvVar(KUBERNETES_CA_CERTIFICATE_FILE_SYSTEM_PROPERTY, config.getCaCertFile()));
						
						config.setOauthToken(Utils.getSystemPropertyOrEnvVar(KUBERNETES_OAUTH_TOKEN_SYSTEM_PROPERTY, config.getOauthToken()));
						config.setUsername(Utils.getSystemPropertyOrEnvVar(KUBERNETES_AUTH_BASIC_USERNAME_SYSTEM_PROPERTY, config.getUsername()));
						config.setPassword(Utils.getSystemPropertyOrEnvVar(KUBERNETES_AUTH_BASIC_PASSWORD_SYSTEM_PROPERTY, config.getPassword()));

					}

					if (!config.masterUrl.toLowerCase(Locale.ROOT).startsWith(HTTP_PROTOCOL_PREFIX) && !config.masterUrl.toLowerCase(Locale.ROOT).startsWith(HTTPS_PROTOCOL_PREFIX)) {
					  config.masterUrl = (SSLUtils.isHttpsAvailable(config) ? HTTPS_PROTOCOL_PREFIX : HTTP_PROTOCOL_PREFIX) + config.masterUrl;
					}

					if (!config.masterUrl.endsWith("/")) {
					  config.masterUrl = config.masterUrl + "/";
					}
					return config;
				}
			}
		}
		
		final String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
		config.setNamespace(namespace);
		final NamespacedKubernetesClient client = new DefaultKubernetesClient(config);
		final int poolSize = flinkConfig.get(KubernetesConfigOptions.KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE);
		
		return new Fabric8FlinkKubeClient(flinkConfig, client, createThreadPoolForAsyncIO(poolSize, useCase));
	}
	return new KubernetesClusterDescriptor( configuration,flinkKubeClient);
}




