





/** 3	FlinkSqlCli 
*
*/



// SqlClient进程中 TableEnvInit初始化和 CatalogManager构建;
// client.start().openSession().build(): ExecutionContext.initializeTableEnvironment()初始化Table环境资源, initializeCatalogs()根据配置生成Catalogs和curdb;
// A. client.start().open().parseCommand(line).sqlParser.parse(stmt): PlannerContext.createCatalogReader() 将CatalogManager中curCatalog/DB作为defaultSchemas 封装进FlinkCatalogReader;
// B. client.start().open().callCommand().callSelect(cmdCall):executor.executeQuery():tableEnv.sqlQuery(selectQuery) 提交Table查询命令: TableEnvironmentImpl.sqlQuery()

SqlClient.main(){
	final SqlClient client = new SqlClient(true, options);
	client.start();{
		final Executor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
        executor.start();
		final Environment sessionEnv = readSessionEnvironment(options.getEnvironment());
        appendPythonConfig(sessionEnv, options.getPythonConfiguration());
		context = new SessionContext(options.getSessionId(), sessionEnv);
		// 创建 ModuleManager, CatalogManager, FunctionCatalog
		String sessionId = executor.openSession(context);{// LocalExecutor.
			String sessionId = sessionContext.getSessionId();// defaul;
			this.contextMap.put(sessionId, createExecutionContextBuilder(sessionContext).build());{//ExecutionContext$Builder.build()
				return new ExecutionContext<>(this.sessionContext,this.sessionState,this.dependencies,,,);{//ExecutionContext()构造函数, 生成一堆的执行环境;
					classLoader = ClientUtils.buildUserCodeClassLoader();
					// 重要的环境变量解析和 运行对象生成
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
							// Step.4 Create catalogs and register them. 基于config配置文件,创建多个Catalog和 curCatalog,curDatabase;
							initializeCatalogs();{// ExecutionContext.initializeCatalogs
								// Step.1 Create catalogs and register them.
								environment.getCatalogs().forEach((name, entry) -> {
												Catalog catalog=createCatalog(name, entry.asMap(), classLoader);
												tableEnv.registerCatalog(name, catalog);{//TableEnvironmentImpl.registerCatalog
													catalogManager.registerCatalog(catalogName, catalog);{//CatalogManager.registerCatalog
														catalog.open();{// 不同的catalog,不同的实现;
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
								// Step.5 Set current catalog and database. 从 
								Optional<String> catalog = environment.getExecution().getCurrentCatalog();// "current-catalog" 参数
								Optional<String> database = environment.getExecution().getCurrentDatabase();// current-database 参数
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
					// 读取一行数据; 
					String line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
					// 1. 解析用户查询语句生成 Calcite对象,并基于默认 curCatalog,curDB生成 FlinkCatalogReader;
					final Optional<SqlCommandCall> cmdCall = parseCommand(line);{//CliClient.
						parsedLine = SqlCommandParser.parse(executor.getSqlParser(sessionId), line);{
							Optional<SqlCommandCall> callOpt = parseByRegexMatching(stmt);
							if (callOpt.isPresent()) {//先用正则解析; 
								return callOpt.get();
							}else{// 没有正则, 进入这里; 
								return parseBySqlParser(sqlParser, stmt);{//SqlCommandParser.parseBySqlParser
									operations = sqlParser.parse(stmt);{//LocalExecutor.Parser匿名类.parse()
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
																// 这里的 currentDatabase,currentDatabase 来源与 CatalogManager.参数; 应该是加载 sql-client-defaults.yaml 后生成的; 
																// 把 currentCatalog("myhive"), currentDatabase("default") 作为默认的 SchemaPaths;
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
					// 2. 提交执行sql Calcite命令; 
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







