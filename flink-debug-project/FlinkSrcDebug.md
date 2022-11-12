# Flink Src Debug 源码解析与追踪

项目目标
* Flink源码编译


## 项目准备

### Flink 源码编译

```$sh
mvn clean install -DskipTests=true -P fast -fae -DskipRat=true
```
mvn clean install 先删除target 再重新编译并install到本地仓库: 
* -DskipTests=true 跳过单元测试
* -P fast, 启用父pom中的id=fast的Profile,这个profile会跳过 rat和checkstyle 检查, 否则会编译失败;
* -fae 参数: 跳过失败的子模板, 单个子模板失败不影响其他的
* -DskipRat=true 通过JVM参数传跳过rat检查的配置, 本项目可忽略; 




