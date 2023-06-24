# hadoop jar /home/adil/hadoop-3.3.5/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -files mapper.py,reducer.py -input file:///home/adil/Python_Important/Projects/Using%20HadoopFrameWork%20Merge%20CSV%20Files/customers-100.csv -output file:///home/adil/Python_Important/Projects/Using%20HadoopFrameWork%20Merge%20CSV%20Files/output -mapper mapper.py -reducer reducer.py

# # correct Url
# hadoop jar /home/adil/hadoop-3.3.5/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -files mapper.py,reducer.py -input file:///home/adil/Python_Important/Projects/Using%20HadoopFrameWork%20Merge%20CSV%20Files/customers-100.csv -output file:///home/adil/Python_Important/Projects/Using%20HadoopFrameWork%20Merge%20CSV%20Files/output -mapper mapper.py -reducer reducer.py





# hadoop jar /home/adil/hadoop-3.3.5/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -files "mapper.py,reducer.py" -input file:/home/adil/Python_Important/Projects/Using%20HadoopFrameWork%20Merge%20CSV%20Files/customers-100.csv -output file:/home/adil/Python_Important/Projects/Using%20HadoopFrameWork%20Merge%20CSV%20Files/output -mapper mapper.py -reducer reducer.py



# ab apne jaise kaha Also, ensure that the input file (customers-100.csv) exists in the specified HDFS directory.

# tu meri (customers-100.csv) file HDFS directory mai user folder ke ander majood hai ha


# hadoop jar /home/adil/hadoop-3.3.5/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -files /home/adil/Python_Important/Projects/HadoopMergeCSV/mapper.py,/home/adil/Python_Important/Projects/HadoopMergeCSV/reducer.py -input hdfs:///user/customers-100.csv -output file:/home/adil/Python_Important/Projects/HadoopMergeCSV/output -mapper mapper.py -reducer reducer.py

#  hadoop jar /home/adil/hadoop-3.3.5/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -files /home/adil/Python_Important/Projects/HadoopMergeCSV/mapper.py,/home/adil/Python_Important/Projects/HadoopMergeCSV/reducer.py -input hdfs:///user/customers-100.csv -output file:/home/adil/Python_Important/Projects/HadoopMergeCSV/output -mapper mapper.py -reducer reducer.py
# packageJobJar: [/tmp/hadoop-unjar7181872489491256628/] [] /tmp/streamjob5351323586046746586.jar tmpDir=null
# 2023-06-21 12:56:42,082 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at /0.0.0.0:8032
# 2023-06-21 12:56:42,357 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at /0.0.0.0:8032
# 2023-06-21 12:56:42,632 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/adil/.staging/job_1687284212271_0013
# 2023-06-21 12:56:43,036 INFO mapred.FileInputFormat: Total input files to process : 1
# 2023-06-21 12:56:43,096 INFO mapreduce.JobSubmitter: number of splits:2
# 2023-06-21 12:56:43,307 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1687284212271_0013
# 2023-06-21 12:56:43,307 INFO mapreduce.JobSubmitter: Executing with tokens: []
# 2023-06-21 12:56:43,524 INFO conf.Configuration: resource-types.xml not found
# 2023-06-21 12:56:43,525 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
# 2023-06-21 12:56:43,598 INFO impl.YarnClientImpl: Submitted application application_1687284212271_0013
# 2023-06-21 12:56:43,641 INFO mapreduce.Job: The url to track the job: http://adil-HP-EliteBook-820-G3:8088/proxy/application_1687284212271_0013/
# 2023-06-21 12:56:43,643 INFO mapreduce.Job: Running job: job_1687284212271_0013
# 2023-06-21 12:56:51,836 INFO mapreduce.Job: Job job_1687284212271_0013 running in uber mode : false
# 2023-06-21 12:56:51,839 INFO mapreduce.Job:  map 0% reduce 0%
# 2023-06-21 12:56:59,007 INFO mapreduce.Job: Task Id : attempt_1687284212271_0013_m_000000_0, Status : FAILED



# 2023-06-21 12:57:13,155 INFO mapreduce.Job: Task Id : attempt_1687284212271_0013_m_000001_2, Status : FAILED
# Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 127
# 	at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:326)
# 	at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:539)
# 	at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
# 	at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:61)
# 	at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
# 	at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:466)
# 	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:350)
# 	at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:178)
# 	at java.base/java.security.AccessController.doPrivileged(Native Method)
# 	at java.base/javax.security.auth.Subject.doAs(Subject.java:423)
# 	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1899)
# 	at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:172)

# 2023-06-21 12:57:21,235 INFO mapreduce.Job:  map 100% reduce 100%
# 2023-06-21 12:57:21,254 INFO mapreduce.Job: Job job_1687284212271_0013 failed with state FAILED due to: Task failed task_1687284212271_0013_m_000000
# Job failed as tasks failed. failedMaps:1 failedReduces:0 killedMaps:0 killedReduces: 0



# 2023-06-21 12:57:21,361 INFO mapreduce.Job: Counters: 14
# 	Job Counters 
# 		Failed map tasks=7
# 		Killed map tasks=1
# 		Killed reduce tasks=1
# 		Launched map tasks=8
# 		Other local map tasks=6
# 		Data-local map tasks=2
# 		Total time spent by all maps in occupied slots (ms)=42646
# 		Total time spent by all reduces in occupied slots (ms)=0
# 		Total time spent by all map tasks (ms)=42646
# 		Total vcore-milliseconds taken by all map tasks=42646
# 		Total megabyte-milliseconds taken by all map tasks=43669504
# 	Map-Reduce Framework
# 		CPU time spent (ms)=0
# 		Physical memory (bytes) snapshot=0
# 		Virtual memory (bytes) snapshot=0
# 2023-06-21 12:57:21,361 ERROR streaming.StreamJob: Job not successful!
# Streaming Command Failed!
# adil@adil-HP-EliteBook-820-G3







# this is mapper.py

#!/usr/bin/env python3

# import sys

# # Assuming the matching field is the first column in each CSV file
# matching_field_index = 0

# for line in sys.stdin:
#     # Remove leading/trailing whitespaces and split the line by comma
#     fields = line.strip().split(',')

#     # Extract the matching field value
#     matching_field = fields[matching_field_index]

#     # Emit the matching field value as the key and the entire record as the value
#     print(f"{matching_field}\t{','.join(fields)}")



# # this is reducer.py

# #!/usr/bin/env python3

# import sys

# current_key = None
# current_records = []

# for line in sys.stdin:
#     key, record = line.strip().split('\t')

#     if current_key is None:
#         current_key = key

#     if key != current_key:
#         # Process the records with the previous key
#         if len(current_records) > 1:
#             merged_record = ','.join(current_records)
#             print(merged_record)

#         # Reset for the new key
#         current_key = key
#         current_records = []

#     current_records.append(record)

# # Process the last set of records
# if len(current_records) > 1:
#     merged_record = ','.join(current_records)
#     print(merged_record)
