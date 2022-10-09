#!/bin/bash
#use Grep&Sum as the base application
function ResetParameters() {
  app="GS_txn"
  FTOptions=0
  failureModel=3
  failureFrequency=6
  firstFailure=10000
  tthreads=16
  snapshot=2

  #System Configurations
  Arrival_Control=1
  targetHz=200000
  Time_Control=1
  time_Interval=4000
  timeSliceLengthMs=1000
  input_store_batch=20000
  systemRuntime=120
  #shellcheck disable=SC2006
  #shellcheck disable=SC2003
  batch_number_per_wm=`expr $input_store_batch \* $tthreads`
  #Workload Configurations
  NUM_ITEMS=163840
  NUM_EVENTS=16000000
  ZIP_SKEW=400
  RATIO_OF_READ=500
  RATIO_OF_ABORT=0
  RATIO_OF_DEPENDENCY=1000
  complexity=0
  NUM_ACCESSES=2
  partition_num_per_txn=16
  partition_num=16
}
function runFTStream() {
 rm -rf /mnt/nvme0n1p2/jjzhao/app/benchmarks/gstxn/
   rm -rf /mnt/nvme0n1p2/jjzhao/app/txngs/checkpoint
   rm -rf /mnt/nvme0n1p2/jjzhao/app/txngs/wal
   echo "java -Xms100g -Xmx100g -jar -XX:+UseG1GC -d64 /home/jjzhao/TSP_PM/StreamProcess/target/StreamProcess-1.0-SNAPSHOT-jar-with-dependencies \
             --app $app \
             --FTOptions $FTOptions \
             --failureModel $failureModel \
             --failureFrequency $failureFrequency \
             --firstFailure $firstFailure \
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
  failureFrequency=6
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
#Partition_num_per_txn
function PartitionNumPerTxnEvaluation() {
  ResetParameters
  systemRuntime=60
  RATIO_OF_DEPENDENCY=500
  failureFrequency=6
  for partition_num_per_txn in 2 4 6 8 10 12 16
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
  for failureFrequency in 2 4 6
  do
  for FTOptions in 1 2 3 4
      do runFTStream
      done
  done
  ResetParameters
  systemRuntime=80
    for failureFrequency in 8 10
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
function ZIPEvaluation() {
ResetParameters
  systemRuntime=80
  partition_num_per_txn=16
  RATIO_OF_READ=0
  RATIO_OF_DEPENDENCY=1000
  for ZIP_SKEW in 1000
do
  for FTOptions in 1 2 3 4
do runFTStream
done
  done
ResetParameters
}
function ReadEvaluation() {
ResetParameters
  systemRuntime=80
  partition_num_per_txn=16
  RATIO_OF_ABORT=0
  RATIO_OF_DEPENDENCY=1000
  for RATIO_OF_READ in 0 250 500 750 1000
do
  for FTOptions in 3 4
do runFTStream
done
  done
ResetParameters
}
#Ratio of dependency
function RatioDependency() {
ResetParameters
  systemRuntime=80
  partition_num_per_txn=16
  RATIO_OF_ABORT=0
  RATIO_OF_READ=0
  for RATIO_OF_DEPENDENCY in 1000
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
  ZIPSkewEvaluation
  ReadRatioEvaluation
  DependencyRatioEvaluation
  PartitionNumPerTxnEvaluation
  ComplexityEvaluation
  CheckpointIntervalEvaluation
}
baselineEvaluation