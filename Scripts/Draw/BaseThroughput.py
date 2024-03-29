import getopt
import os
import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pylab
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator, LogLocator, MaxNLocator
from numpy import double

FILE_FOLDER = "/home/jjzhao/app/results"

def ReadFileThroughput(apps, FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num):
# app:[0, 1, 2, 3, 4]
    w = 6
    y = [[] for _ in range(w)]

    for FTOptions in FTOptions:
        RATIO_OF_READ = 950
        op_path = getPathApp("GS_txn", FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    for FTOptions in FTOptions:
        RATIO_OF_READ = 950
        op_path = getPathApp("GS_txn", FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for FTOptions in FTOptions:
        RATIO_OF_READ = 50
        op_path = getPathApp("GS_txn", FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    for FTOptions in FTOptions:
        RATIO_OF_READ = 500
        op_path = getPathApp("OB_txn", FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[3].append(float(throughput))

    for FTOptions in FTOptions:
        RATIO_OF_READ = 700
        NUM_ITEMS = 81920
        op_path = getPathApp("SL_txn", FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[4].append(float(throughput))

    for FTOptions in FTOptions:
        RATIO_OF_READ = 500
        NUM_ITEMS = 40960
        op_path = getPathApp("TP_txn", FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[5].append(float(throughput))

    print(y)


def getPathApp(app, FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num):
    return FILE_FOLDER + '/Application={}/FTOptions={}/Exactly_Once={}/Arrival_Control={}/failureTime={}_targetHz={}_NUM_EVENTS={}_NUM_ITEMS={}_NUM_ACCESSES={}_ZIP={}_RATIO_OF_READ={}_RATIO_OF_ABORT={}_RATIO_OF_DEPENDENCY={}_partition_num_per_txn={}_partition_num={}'\
         .format(app, FTOptions, Exactly_Once, Arrival_Control, failureTime, targetHz, NUM_EVENTS, NUM_ITEMS, NUM_ACCESSES, ZIP_SKEW, RATIO_OF_READ, RATIO_OF_ABORT, RATIO_OF_DEPENDENCY, partition_num_per_txn, partition_num)

if __name__ == '__main__':
    Exactly_Once="false"
    Arrival_Control="false"
    failureTime=0
    targetHz=200000
    NUM_EVENTS=8000000
    NUM_ITEMS=163840
    NUM_ACCESSES=2
    ZIP_SKEW=400
    RATIO_OF_READ=500
    RATIO_OF_ABORT=0
    RATIO_OF_DEPENDENCY=500
    partition_num_per_txn=2
    partition_num=16

    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:e:i:a:z:r:ab:d:p:t")
    except getopt.GetoptError:
        print
    for opt, arg in opts:
        if opt in ['-t']:
            targetHz = int(arg)
        elif opt in ['-e']:
            NUM_EVENTS = int(arg)
        elif opt in ['-i']:
            NUM_ITEMS = int(arg)
        elif opt in ['-a']:
            NUM_ACCESSES = int(arg)
        elif opt in ['-z']:
            ZIP_SKEW = int(arg)
        elif opt in ['-r']:
            RATIO_OF_READ = int(arg)
        elif opt in ['-ab']:
            RATIO_OF_ABORT = int(arg)
        elif opt in ['-d']:
            RATIO_OF_DEPENDENCY = int(arg)
        elif opt in ['-p']:
            partition_num_per_txn = int(arg)
        elif opt in ['-t']:
            partition_num = int(arg)

    apps = ["GS_txn", "TP_txn", "SL_txn", "OB_txn"]
    FTOptions = [0, 1, 2, 5, 6]



