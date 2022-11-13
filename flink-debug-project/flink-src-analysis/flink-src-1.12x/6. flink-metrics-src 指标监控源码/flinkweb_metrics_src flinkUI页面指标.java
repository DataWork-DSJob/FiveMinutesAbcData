




查看 反压指标的 api 

http://hadoop1:8088/proxy/application_1660273546536_5290/jobs/3cc70d90b64e628048fa824be668681e/vertices/2fb3a848af2d78ef8572af6cb286a035/backpressure




checkpoint相关信息：
lastCheckpointDuration： 最近完成checkpoint的时长
lastCheckpointSize: 最近完成checkpoint的大小
lastCheckpointRestoreTimestamp： 作业失败后恢复的能力
numberOfCompletedCheckpoints，numberOfFailedCheckpoints： 成功和失败的checkpoint数目
checkpointAlignmentTime： Exactly once模式下barrier对齐时间




sum(flink_taskmanager_job_task_operator_KafkaConsumer_topic_partition_currentOffsets -flink_taskmanager_job_task_operator_KafkaConsumer_topic_partition_committedOffsets ) by (job_name) 




flink_taskmanager_job_task_operator_KafkaConsumer_topic_partition_currentOffsets{job_name=~"$job_name",task_attempt_id=~"$task_attempt_id",partition=~"$partition"} - flink_taskmanager_job_task_operator_KafkaConsumer_topic_partition_committedOffsets{job_name=~"$job_name",task_attempt_id=~"$task_attempt_id",partition=~"$partition"}


flink_taskmanager_job_task_operator_KafkaConsumer_topic_partition_currentOffsets{job_name=~"$job_name",task_name=~"$task_name",partition=~"$partition"} - flink_taskmanager_job_task_operator_KafkaConsumer_topic_partition_committedOffsets{job_name=~"$job_name",task_name=~"$task_name",partition=~"$partition"} 
