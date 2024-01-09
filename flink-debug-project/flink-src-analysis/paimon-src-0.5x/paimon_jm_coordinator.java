
// 这个似乎是 JM端 定时checkpoint后 跟踪确认 paimon 数据源是否要 查询新数据的; 

nextPlan:135, InnerStreamTableScanImpl (org.apache.paimon.table.source)
plan:95, InnerStreamTableScanImpl (org.apache.paimon.table.source)
scanNextSnapshot:167, ContinuousFileSplitEnumerator (org.apache.paimon.flink.source)


scheduleAtFixedRate:570, ScheduledThreadPoolExecutor (java.util.concurrent)
notifyReadyAsync:127, ExecutorNotifier (org.apache.flink.runtime.source.coordinator)
callAsync:312, SourceCoordinatorContext (org.apache.flink.runtime.source.coordinator)
start:113, ContinuousFileSplitEnumerator (org.apache.paimon.flink.source)
lambda$start$1:225, SourceCoordinator (org.apache.flink.runtime.source.coordinator)


runInCoordinatorThread:326, SourceCoordinatorContext (org.apache.flink.runtime.source.coordinator)
runInEventLoop:446, SourceCoordinator (org.apache.flink.runtime.source.coordinator)
start:225, SourceCoordinator (org.apache.flink.runtime.source.coordinator)
applyCall:315, RecreateOnResetOperatorCoordinator$DeferrableCoordinator (org.apache.flink.runtime.operators.coordination)
start:70, RecreateOnResetOperatorCoordinator (org.apache.flink.runtime.operators.coordination)
start:181, OperatorCoordinatorHolder (org.apache.flink.runtime.operators.coordination)
startOperatorCoordinators:165, DefaultOperatorCoordinatorHandler (org.apache.flink.runtime.scheduler)
startAllOperatorCoordinators:82, DefaultOperatorCoordinatorHandler (org.apache.flink.runtime.scheduler)
startScheduling:615, SchedulerBase (org.apache.flink.runtime.scheduler)
startScheduling:1044, JobMaster (org.apache.flink.runtime.jobmaster)
startJobExecution:961, JobMaster (org.apache.flink.runtime.jobmaster)
onStart:424, JobMaster (org.apache.flink.runtime.jobmaster)




