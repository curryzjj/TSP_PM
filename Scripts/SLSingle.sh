#!/bin/bash
function ResetParameters() {
  app="SL_txn"
  FTOptions=0
  failureModel=0
  failureFrequency=0.0
  tthreads=16
  snapshot=10

  #System Configurations
  Arrival_Control=1
  targetHz=300000.0
  timeSliceLengthMs=1000
  input_store_batch=10000
  #shellcheck disable=SC2006
  #shellcheck disable=SC2003
  batch_number_per_wm=`expr $input_store_batch \* $tthreads`
  #Workload Configurations
  NUM_ITEMS=163840
  NUM_EVENTS=16000000
  ZIP_SKEW=0.4
  RATIO_OF_READ=500
  RATIO_OF_ABORT=0
  RATIO_OF_DEPENDENCY=500
  complexity=0
  NUM_ACCESSES=2
  partition_num_per_txn=2
  partition_num=16
}
function runFTStream() {
  echo " java -Xms100g -Xmx100g -jar -XX:+UseG1GC -d64 /home/jjzhao/TSP_PM/StreamProcess/target/StreamProcess-1.0-SNAPSHOT-jar-with-dependencies \
            --app $app \
            --FTOptions $FTOptions \
            --failureModel $failureModel \
            --failureFrequency $failureFrequency \
            --tthreads $tthreads \
            --snapshot $snapshot \
            --Arrival_Control $Arrival_Control \
            --targetHz $targetHz \
            --timeSliceLengthMs $timeSliceLengthMs \
            --input_store_batch $input_store_batch \
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
              --timeSliceLengthMs $timeSliceLengthMs \
              --input_store_batch $input_store_batch \
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
function baselineEvaluation() {
  ResetParameters
    for FTOptions in 0 1 2 3 4
        do runFTStream
        done
}
baselineEvaluation