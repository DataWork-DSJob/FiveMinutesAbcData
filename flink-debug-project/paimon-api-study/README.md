# Flink 1.15 Version Notes


1. 不再支持 scala_2.11, 为确保savepoint,依赖数据等一致性; 从1.15后所有scala版本必需是2.12的; 
2. 大部分模块(flink-cep, flink-clients, flink-connects-xx) 都重命名, 去掉了_scale_version的后缀; 
3. Java DataSet/-Stream APIs 独立出来, 不依赖scala版本; 
4. 默认支持jdk11, 推荐升级jdk11; 
5. Table api 变化
   - The previously deprecated methods TableEnvironment.execute, Table.insertInto, TableEnvironment.fromTableSource, TableEnvironment.sqlUpdate, and TableEnvironment.explain have been removed. 
   - Please use TableEnvironment.executeSql, TableEnvironment.explainSql, TableEnvironment.createStatementSet, as well as Table.executeInsert, Table.explain and Table.execute and the newly introduces classes TableResult, ResultKind, StatementSet and ExplainDetail.
   - 统一到单一配置类TableConfig
6.依赖版本升级: 
   - 最低Hadoop支持版本升到 2.8.5; Upgrade the minimal supported hadoop version to 2.8.5; 
   - 要求zk最低版本3.5以上; 



参考: https://nightlies.apache.org/flink/flink-docs-release-1.15/release-notes/flink-1.15/#summary-of-changed-dependency-names

