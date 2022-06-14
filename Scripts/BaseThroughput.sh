#!/bin/bash
function ResetParameters() {
  app="GS_txn"
  FTOptions=0
  failureModel=0
  failureFrequency=0.0
  tthreads=24

  #System Configurations
  Arrival_Control=1
  targetHz=300000.0
  timeSliceLengthMs=1000
  input_store_batch=5000
  #shellcheck disable=SC2006
  #shellcheck disable=SC2003
  batch_number_per_wm=`expr $input_store_batch \* $tthreads`
  #Workload Configurations
  NUM_ITEMS=10000
  NUM_EVENTS=900000
  ZIP_SKEW=0.4
  RATIO_OF_READ=75
  NUM_ACCESSES=2
  partition_num_per_txn=2
  partition_num=$tthreads
}
function runFTStream() {
  echo "java -Xms300g -Xmx300g -jar -d64 /home/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
            --app $app \
            --FTOptions $FTOptions \
            --failureModel $failureModel \
            --failureFrequency $failureFrequency \
            --tthreads $tthreads \
            --Arrival_Control $Arrival_Control \
            --targetHz $targetHz \
            --timeSliceLengthMs $timeSliceLengthMs \
            --input_store_batch $input_store_batch \
            --batch_number_per_wm $batch_number_per_wm \
            --NUM_ITEMS $NUM_ITEMS \
            --NUM_EVENTS $NUM_EVENTS \
            --ZIP_SKEW $ZIP_SKEW \
            --RATIO_OF_READ $RATIO_OF_READ \
            --NUM_ACCESSES $NUM_ACCESSES \
            --partition_num_per_txn $partition_num_per_txn \
            --partition_num $partition_num
            "
  java -Xms300g -Xmx300g -jar -d64 /home/jjzhao/MorphStream/application/target/application-0.0.2-jar-with-dependencies.jar \
              --app $app \
              --FTOptions $FTOptions \
              --failureModel $failureModel \
              --failureFrequency $failureFrequency \
              --tthreads $tthreads \
              --Arrival_Control $Arrival_Control \
              --targetHz $targetHz \
              --timeSliceLengthMs $timeSliceLengthMs \
              --input_store_batch $input_store_batch \
              --batch_number_per_wm $batch_number_per_wm \
              --NUM_ITEMS $NUM_ITEMS \
              --NUM_EVENTS $NUM_EVENTS \
              --ZIP_SKEW $ZIP_SKEW \
              --RATIO_OF_READ $RATIO_OF_READ \
              --NUM_ACCESSES $NUM_ACCESSES \
              --partition_num_per_txn $partition_num_per_txn \
              --partition_num $partition_num
}
function baselineEvaluation() {
  ResetParameters
  for app in GS_txn TP_txn SL_txn OB_txn
  do
    for FTOptions in 0 1 2 3 4
    do runFTStream
    done
  done
}