package UserApplications;

import System.util.OsUtils;
import com.beust.jcommander.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import static System.Constants.*;

public abstract class baseRunner {
    public static final Logger LOG= LoggerFactory.getLogger(baseRunner.class);
    protected static String CFG_PATH = null;
    @Parameter(names={"-a","--app"},description = "The application to be executed",required = false)
    public String application = "GS_txn";
    //public String application = "TP_txn";
    //public String application="OB_txn";
    //public String application="SL_txn";

    //public String application = "WordCount";
    @Parameter(names = {"-t", "--Brisk.topology-name"}, required = false, description = "The name of the Brisk.topology")
    public String topologyName;
    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = "";
    @Parameter(names = {"--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;


    @Parameter(names = {"--measure"}, description = "measure enable")
    public boolean measure = false;

    @Parameter(names = {"--shared"}, description = "shared by multi producers")
    public boolean shared = false;
    @Parameter(names = {"--common"}, description = "common shared by consumers")
    public boolean common = false;
    @Parameter(names = {"--linked"}, description = "linked")
    public boolean linked = false;

    @Parameter(names = {"--native"}, description = "native execution")
    public boolean NAV = true;
    @Parameter(names = {"--profile"}, description = "profiling")
    public boolean profile = false;
    @Parameter(names = {"--benchmark"}, description = "benchmarking the throughput of all applications")
    public boolean benchmark = false;
    @Parameter(names = {"--load"}, description = "benchmarking the throughput of all applications")
    public boolean load = false;
    @Parameter(names = {"--microbenchmark"}, description = "benchmarking the throughput of all applications")
    public boolean microbenchmark = false;

    @Parameter(names = {"--r", "--runtime"}, description = "Runtime in seconds for the Brisk.topology (local mode only)")
    public int runtimeInSeconds = 30;

    @Parameter(names = {"--bt"}, description = "fixed batch", required = false)
    public int batch = 1000;

    @Parameter(names = {"--tt"}, description = "parallelism", required = false)
    public int tthread = 1;
    @Parameter(names = {"--DataBase"}, description = "DataBase", required = false)
    public String DataBase= "in-memory";

    //FaultTolerance
    @Parameter(names = {"--FTOptions"}, description = "Which fault tolerance option, 0: no FT, 1: Wal, 2: Checkpoint, 3: CLR", required = false)
    public int FTOptions = 0;
    @Parameter(names = {"--failureModel"}, description = "No failure(0), Transaction Abort(1), State lost(2), Both(3)", required = false)
    public int failureModel= 0;
    @Parameter(names = {"--failureFrequency"}, description = "Failure Frequency", required = false)
    public double failureFrequency =0.4;
    @Parameter(names = {"--Exactly_Once"}, description = "is Exactly_Once", required = false)
    public int Exactly_Once = 0;

    //Workload Configuration
    @Parameter(names = {"--Arrival_Control"}, description = "is Arrival_Control", required = false)
    public int Arrival_Control = 1;
    @Parameter(names = {"--targetHz"}, description = "Arrive rate(events / s)", required = false)
    public double targetHz = 200000.0;
    @Parameter(names = {"--NUM_ITEMS"}, description = "Number of items in the table", required = false)
    public int NUM_ITEMS = 10000;
    @Parameter(names = {"--NUM_EVENTS"}, description = "Total events", required = false)
    public int NUM_EVENTS = 300000;
    @Parameter(names = {"--NUM_ACCESSES"}, description = "Number access per transaction", required = false)
    public int NUM_ACCESSES = 1;
    @Parameter(names = {"--partition_num"}, description = "Number of partition", required = false)
    public int partition_num = 1;
    @Parameter(names = {"--partition_num_per_txn"}, description = "Number of partition to access per transaction", required = false)
    public int partition_num_per_txn = 1;
    @Parameter(names = {"--ZIP_SKEW"}, description = "ZIP_SKEW", required = false)
    public double ZIP_SKEW = 0.4;
    @Parameter(names = {"--RATIO_OF_READ"}, description = "RATIO_OF_READ", required = false)
    public int ratioOfRead = 750;
    @Parameter(names = {"--RATIO_OF_ABORT"}, description = "RATIO_OF_ABORT", required = false)
    public int ratioOfAbort = 100;
    @Parameter(names = {"--RATIO_OF_DEPENDENCY"}, description = "RATIO_OF_DEPENDENCY", required = false)
    public int ratioOfDependency = 1000;
    @Parameter(names = {"--complexity"}, description = "complexity", required = false)
    public int complexity = 0;

    //System Configuration
    @Parameter(names = {"--tthreads"}, description = "parallelism", required = false)
    public int tthreads = 4;
    @Parameter(names = {"--timeSliceLengthMs"}, description = "timeSliceLengthMs for arrive rate", required = false)
    public int timeSliceLengthMs = 1000;
    @Parameter(names = {"--input_store_batch"}, description = "input_store_batch", required = false)
    public int input_store_batch = 5000;
    @Parameter(names = {"--batch_number_per_wm"}, description = "batch_number_per_wm", required = false)
    public int batch_number_per_wm = 5000;
    @Parameter(names = {"--isParallel"}, description = "isParallel to store", required = false)
    public int isParallel = 0;
    @Parameter(names = {"--spoutThread"}, description = "Number of spout", required = false)
    public int spoutThread = 1;
    @Parameter(names = {"--sinkThread"}, description = "Number of sink", required = false)
    public int sinkThread = 1;

    //Algorithm Configuration
    @Parameter(names = {"--enable_time_Interval"}, description = "time interval or number interval", required = false)
    public int enable_time_Interval = 0;
    @Parameter(names = {"--time_Interval"}, description = "time interval", required = false)
    public int time_Interval = 6000;
    @Parameter(names = {"--snapshot"}, description = "batch per commit(number interval)", required = false)
    public int snapshot = 10;



    public  baseRunner() {
        if(OsUtils.isMac()){
            CFG_PATH = Mac_Project_Path + "/Common/src/main/resources/config/%s.properties";
            metric_path = Mac_Measure_Path;
        }else{
            CFG_PATH = Node22_Project_Path + "/Common/src/main/resources/config/%s.properties";
            metric_path = Node22_Measure_Path;
        }
    }
    public static Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is;
        is = new FileInputStream(filename);
        properties.load(is);
        is.close();
        return properties;
    }
    public void setConfiguration(HashMap<String,Object> config){
        config.put("benchmark", benchmark);
        config.put("profile", profile);
        config.put("NAV", NAV);
        config.put("application",application);

        config.put("measure", measure);

        config.put("linked", linked);
        config.put("shared",shared);
        config.put("common",common);
        config.put("batch",1);


        config.put("microbenchmark", microbenchmark);
        config.put("metrics.output", metric_path);

        config.put("runtimeInSeconds", runtimeInSeconds);
        config.put("DataBase",DataBase);
        config.put("Sequential_Binding",true);

        //Fault tolerance
        config.put("FTOptions", FTOptions);
        config.put("failureModel",failureModel);
        config.put("failureFrequency",failureFrequency);
        if (Exactly_Once == 1){
            config.put("Exactly_Once",true);
        } else {
            config.put("Exactly_Once",false);
        }
        //Workload Configuration
        if (Arrival_Control == 1){
            config.put("Arrival_Control",true);
        } else {
            config.put("Arrival_Control",false);
        }
        config.put("targetHz",targetHz);
        config.put("NUM_ITEMS",NUM_ITEMS);
        config.put("NUM_EVENTS",NUM_EVENTS);
        config.put("NUM_ACCESSES",NUM_ACCESSES);
        config.put("partition_num",partition_num);
        config.put("partition_num_per_txn",partition_num_per_txn);
        config.put("ZIP_SKEW",ZIP_SKEW);
        config.put("RATIO_OF_READ", ratioOfRead);
        config.put("RATIO_OF_ABORT", ratioOfAbort);
        config.put("RATIO_OF_DEPENDENCY", ratioOfDependency);
        config.put("complexity",complexity);
        //System Configuration
        config.put("tthreads",tthreads);
        config.put("executor.threads",tthreads);
        config.put("timeSliceLengthMs",timeSliceLengthMs);
        config.put("input_store_batch",input_store_batch);
        config.put("batch_number_per_wm",batch_number_per_wm);
        config.put("spoutThread",spoutThread);
        config.put("sinkThread",sinkThread);
        if (isParallel == 1){
            config.put("isParallel",true);
        } else {
            config.put("isParallel",false);
        }
        //Algorithm Configuration
        if (enable_time_Interval == 1){
            config.put("enable_time_Interval",true);
        } else {
            config.put("enable_time_Interval",false);
        }
        config.put("time_Interval",time_Interval);
        config.put("snapshot",snapshot);
    }
}
