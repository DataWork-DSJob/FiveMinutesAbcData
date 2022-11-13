






// 1.3 K8s Cli
// KubernetesSessionCli.main() 的Java提交命令
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
			}else{// 第一次,进入这里; 
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







