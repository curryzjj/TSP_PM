#!/bin/bash
#use Grep&Sum as the base application
function ResetParameters() {
  app="GS_txn"
  FTOptions=0
  failureModel=2
  failureFrequency=4
  tthreads=16
  snapshot=2

  #System Configurations
  Arrival_Control=1
  targetHz=200000
  Time_Control=1
  time_Interval=3000
  timeSliceLengthMs=1000
  input_store_batch=20000
  systemRuntime=80
  #shellcheck disable=SC2006
  #shellcheck disable=SC2003
  batch_number_per_wm=`expr $input_store_batch \* $tthreads`
  #Workload Configurations
  NUM_ITEMS=163840
  NUM_EVENTS=16000000
  ZIP_SKEW=400
  RATIO_OF_READ=500
  RATIO_OF_ABORT=0
  RATIO_OF_DEPENDENCY=500
  complexity=0
  NUM_ACCESSES=2
  partition_num_per_txn=8
  partition_num=16
  sudo rm -rf /mnt/nvme0n1p2/jjzhao/app/benchmarks/gstxn/
  sudo rm -rf /mnt/nvme0n1p2/jjzhao/app/txngs/checkpoint
  sudo rm -rf /mnt/nvme0n1p2/jjzhao/app/txngs/wal
}
function runFTStream() {
  echo "java -Xms100g -Xmx100g -jar -XX:+UseG1GC -d64 /home/jjzhao/TSP_PM/StreamProcess/target/StreamProcess-1.0-SNAPSHOT-jar-with-dependencies \
            --app $app \
            --FTOptions $FTOptions \
            --failureModel $failureModel \
            --failureFrequency $failureFrequency \
            --tthreads $tthreads \
            --snapshot $snapshot \
            --Arrival_Control $Arrival_Control \
            --targetHz $targetHz \
            --Time_Control $Time_Control \
            --time_Interval $time_Interval \
            --timeSliceLengthMs $timeSliceLengthMs \
            --input_store_batch $input_store_batch \
            --systemRuntime $systemRuntime \
            --batch_number_per_wm $batch_number_per_wm \
            --NUM_ITEMS $NUM_ITEMS \
            --NUM_EVENTS $NUM_EVENTS \
            --ZIP_SKEW $ZIP_SKEW \
            --RATIO_OF_READ $RATIO_OF_READ \
            --RATIO_OF_ABORT $RATIO_OF_ABORT \
            --RATIO_OF_DEPENDENCY $RATIO_OF_DEPENDENCY \
            --complexity $complexity \
            --NUM_ACCESSES $NUM_ACCESSES \
            --partition_num_per_txn $partition_num_per_txn \
            --partition_num $partition_num
            "
  java -Xms100g -Xmx100g -jar -XX:+UseG1GC -d64 /home/jjzhao/TSP_PM/StreamProcess/target/StreamProcess-1.0-SNAPSHOT-jar-with-dependencies.jar \
              --app $app \
              --FTOptions $FTOptions \
              --failureModel $failureModel \
              --failureFrequency $failureFrequency \
              --tthreads $tthreads \
              --snapshot $snapshot \
              --Arrival_Control $Arrival_Control \
              --targetHz $targetHz \
              --Time_Control $Time_Control \
              --time_Interval $time_Interval \
              --timeSliceLengthMs $timeSliceLengthMs \
              --input_store_batch $input_store_batch \
              --systemRuntime $systemRuntime \
              --batch_number_per_wm $batch_number_per_wm \
              --NUM_ITEMS $NUM_ITEMS \
              --NUM_EVENTS $NUM_EVENTS \
              --ZIP_SKEW $ZIP_SKEW \
              --RATIO_OF_READ $RATIO_OF_READ \
              --RATIO_OF_ABORT $RATIO_OF_ABORT \
              --RATIO_OF_DEPENDENCY $RATIO_OF_DEPENDENCY \
              --complexity $complexity \
              --NUM_ACCESSES $NUM_ACCESSES \
              --partition_num_per_txn $partition_num_per_txn \
              --partition_num $partition_num
}
#Arrival Rate
function ArrivalRateEvaluation() {
  ResetParameters
  systemRuntime=60
  for targetHz in 100000 150000 200000 250000 300000
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#Table Size
function TableSizeEvaluation() {
  ResetParameters
  systemRuntime=60
  for NUM_ITEMS in 20480 40960 81920 163840 327680 655360
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#ZIP_Skew
function TableSizeEvaluation() {
  ResetParameters
  systemRuntime=60
  for ZIP_SKEW in 200 400 600 800 1000
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#Ratio of read
function ReadRatioEvaluation() {
  ResetParameters
  systemRuntime=60
  for RATIO_OF_READ in 200 400 600 800 1000
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#Ratio of dependency
function DependencyRatioEvaluation() {
  ResetParameters
  systemRuntime=60
  partition_num_per_txn=8
  for RATIO_OF_DEPENDENCY in 200 400 600 800 1000
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#Partition_num_per_txn
function PartitionNumPerTxnEvaluation() {
  ResetParameters
  systemRuntime=60
  RATIO_OF_DEPENDENCY=500
  for partition_num_per_txn in 2 4 6 8 10 12 16
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#Complexity
function ComplexityEvaluation() {
  ResetParameters
  systemRuntime=60
  for complexity in 0 20000 40000 60000 80000 10000
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#failureFrequency
function FailureFrequencyEvaluation() {
  ResetParameters
  systemRuntime=60
  for failureFrequency in 1 2 3 4 5 6 7 8 9 10
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
#CheckpointInterval
function CheckpointIntervalEvaluation() {
  ResetParameters
  systemRuntime=60
  for time_Interval in 1000 2000 3000 4000 5000
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
}
function baselineEvaluation() {
  ResetParameters
  ArrivalRateEvaluation
  TableSizeEvaluation
  ReadRatioEvaluation
  DependencyRatioEvaluation
  PartitionNumPerTxnEvaluation
  ComplexityEvaluation
  CheckpointIntervalEvaluation
}
baselineEvaluation