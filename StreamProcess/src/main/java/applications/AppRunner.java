package applications;

import System.Platform.Platform;
import System.measure.MeasureTools;
import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.CONTROL;
import UserApplications.baseRunner;
import applications.topology.WordCount;
import applications.topology.transactional.GS_txn;
import applications.topology.transactional.OB_txn;
import applications.topology.transactional.SL_txn;
import applications.topology.transactional.TP_txn;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TopologySubmitter;
import streamprocess.execution.runtime.threads.executorThread;

import java.io.IOException;
import java.util.*;

import static UserApplications.CONTROL.*;

public class  AppRunner extends baseRunner {
    private static final Logger LOG= LoggerFactory.getLogger(AppRunner.class);
    private static Topology final_topology;
    private final AppDriver driver;
    private final Configuration config=new Configuration();
    private Platform p;
    private AppRunner(){
        driver=new AppDriver();
        driver.addApp("WordCount", WordCount.class);
        driver.addApp("TP_txn", TP_txn.class);
        driver.addApp("GS_txn", GS_txn.class);
        driver.addApp("OB_txn", OB_txn.class);
        driver.addApp("SL_txn", SL_txn.class);

    }
    private void LoadConfiguration() {
        //Get the running environment
        if(OsUtils.isMac()){
            LOG.info(application+"running on the mac");
        }else {
            LOG.info(application+"running on the Node22");
        }
        // Loads the configuration file set by the user or the default
        // configuration
        // Prepared default configuration
        setConfiguration(config);
        if (configStr == null){
            String cfg = String.format(CFG_PATH,application);
            //String ftcfg = String.format(CFG_PATH,"FTConfig");
            Properties p = null;
            try {
                p = loadProperties(cfg);
                config.putAll(Configuration.fromProperties(p));
                //p=loadProperties(ftcfg);
                //config.putAll(Configuration.fromProperties(p));
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.metric_path = this.metric_path + application;
        }
        //Set the fault tolerance mechanisms
        switch (config.getInt("FTOptions")){
            case 0:
                break;
            case 1:
                CONTROL.enable_wal = true;
                CONTROL.enable_input_store = true;
                CONTROL.enable_snapshot = true;
                CONTROL.enable_undo_log = true;
                CONTROL.enable_parallel = true;
                break;
            case 2:
                CONTROL.enable_checkpoint = true;
                CONTROL.enable_input_store = true;
                CONTROL.enable_snapshot = true;
                CONTROL.enable_undo_log = true;
                CONTROL.enable_parallel = true;
                break;
            case 3:
                CONTROL.enable_clr = true;
                CONTROL.enable_input_store = true;
                CONTROL.enable_snapshot = true;
                CONTROL.enable_undo_log = true;
                CONTROL.enable_parallel = true;
                CONTROL.enable_upstreamBackup = true;
                CONTROL.enable_align_wait = true;
                CONTROL.enable_recovery_dependency = true;
                break;
            case 4:
                CONTROL.enable_clr = true;
                CONTROL.enable_input_store = true;
                CONTROL.enable_snapshot = true;
                CONTROL.enable_undo_log = true;
                CONTROL.enable_parallel = true;
                CONTROL.enable_upstreamBackup = true;
                CONTROL.enable_align_wait = true;
                CONTROL.enable_determinants_log = true;
                break;
            case 5:
                CONTROL.enable_clr = true;
                CONTROL.enable_input_store = true;
                CONTROL.enable_snapshot = true;
                CONTROL.enable_undo_log = true;
                CONTROL.enable_parallel = true;
                CONTROL.enable_upstreamBackup = true;
                CONTROL.enable_spoutBackup = true;
                CONTROL.enable_align_wait = true;
                CONTROL.enable_recovery_dependency = true;
                break;
            case 6:
                CONTROL.enable_clr = true;
                CONTROL.enable_input_store = true;
                CONTROL.enable_snapshot = true;
                CONTROL.enable_undo_log = true;
                CONTROL.enable_parallel = true;
                CONTROL.enable_upstreamBackup = true;
                CONTROL.enable_spoutBackup = true;
                CONTROL.enable_align_wait = true;
                CONTROL.enable_determinants_log = true;
                break;
        }
        //Set the failure model
        switch (config.getInt("failureModel",0)){
            case 0:
                break;
            case 1:
                CONTROL.enable_transaction_abort = true;
                break;
            case 2:
                CONTROL.enable_states_lost = true;
                break;
            case 3:
                CONTROL.enable_transaction_abort = CONTROL.enable_states_lost = true;
                break;
        }
        //Set the application
       CONTROL.Time_Control = config.getBoolean("enable_time_Interval");
       CONTROL.Arrival_Control = config.getBoolean("Arrival_Control");
       CONTROL.RATIO_OF_READ = config.getInt("RATIO_OF_READ");
       CONTROL.RATIO_OF_DEPENDENCY = config.getInt("RATIO_OF_DEPENDENCY");
       CONTROL.RATIO_OF_ABORT = config.getInt("RATIO_OF_ABORT");
       CONTROL.NUM_ACCESSES = config.getInt("NUM_ACCESSES");
       CONTROL.NUM_ITEMS = config.getInt("NUM_ITEMS");
       CONTROL.NUM_EVENTS = config.getInt("NUM_EVENTS");
       CONTROL.ZIP_SKEW = config.getDouble("ZIP_SKEW");
       CONTROL.PARTITION_NUM = config.getInt("partition_num");
       CONTROL.Exactly_Once = config.getBoolean("Exactly_Once");
       CONTROL.COMPLEXITY = config.getInt("complexity");
       //Set failure time, starts after five seconds, Adjust the failure interval according to different failure frequencies
        if (CONTROL.enable_states_lost && config.getInt("failureFrequency") > 0) {
            int interval;
            interval = 100 / config.getInt("failureFrequency");
            for (int i = 0; i < config.getInt("failureFrequency"); i++) {
                CONTROL.failureTimes.add(10000 + i * interval * 1000);
            }
            CONTROL.failureTime = failureTimes.poll();
        }
    }

    private static double runTopologyLocally(Topology topology,Configuration conf) throws UnhandledCaseException, InterruptedException, IOException {
        TopologySubmitter submitter=new TopologySubmitter();
        final_topology = submitter.submitTopology(topology,conf);
        executorThread sinkThread = submitter.getOM().getEM().getSinkThread();
        long start = System.currentTimeMillis();
        sinkThread.join((long) (12 * 1E3 * 60));//sync_ratio for sink thread to stop. Maximally sync_ratio for 10 mins
        long time_elapsed = (long) ((System.currentTimeMillis() - start) / 1E3 / 60);//in mins
        if (time_elapsed > 20) {
            LOG.info("Program error, exist...");
            System.exit(-1);
        }
        Thread.sleep((long) (3 * 1E3 * 1));
        submitter.getOM().join();
        try {
            final_topology.db.close();
            if(enable_wal|| enable_checkpoint ||enable_clr){
                submitter.getOM().getEM().closeFTM();
            }
            submitter.getOM().getEM().exit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private void run() throws UnhandledCaseException, InterruptedException, IOException {
        LoadConfiguration();
        //set the MeasureTool
        MeasureTools tools = new MeasureTools(config.getInt("partition_num"), config.getInt("executor.threads"), config.getInt("FTOptions"));
        //Get the descriptor for thr given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        // In case topology names is given, create one
        if (topologyName == null) {
            topologyName = application;
        }
        //Get the topology
        Topology topology = app.getTopology(topologyName,config);
        topology.addMachine(p);
        //Run the topology
        double rt = runTopologyLocally(topology,config);
        // decide the output path of metrics.
        String directory;
        String statsFolderPattern = config.getString("metrics.output")
                + OsUtils.osWrapperPostFix("Application=%s")
                + OsUtils.osWrapperPostFix("NUM_EVENTS=%d_NUM_ITEMS=%d_NUM_ACCESSES=%d_ZIP=%d_RATIO_OF_READ=%d_RATIO_OF_ABORT=%d_RATIO_OF_DEPENDENCY=%d_partition_num_per_txn=%d_partition_num=%d")
                + OsUtils.osWrapperPostFix("Exactly_Once=%s_Arrival_Control=%s_targetHz=%d_TimeControl=%s_timeInterval=%d_InputStoreBatch=%d_failureModel=%d_failureTime=%d")
                + OsUtils.osWrapperPostFix("FTOption=%d");
        directory = String.format(statsFolderPattern,
                config.getString("application"),
                config.getInt("NUM_EVENTS"),
                config.getInt("NUM_ITEMS"),
                config.getInt("NUM_ACCESSES"),
                ZIP_SKEW,
                config.getInt("RATIO_OF_READ"),
                config.getInt("RATIO_OF_ABORT"),
                config.getInt("RATIO_OF_DEPENDENCY"),
                config.getInt("partition_num_per_txn"),
                config.getInt("partition_num"),
                config.getBoolean("Exactly_Once"),
                config.getBoolean("Arrival_Control"),
                config.getInt("targetHz"),
                config.getBoolean("enable_time_Interval"),
                config.getInt("time_Interval"),
                config.getInt("input_store_batch"),
                config.getInt("failureModel"),
                config.getInt("failureFrequency"),
                config.getInt("FTOptions"));
        MeasureTools.METRICS_REPORT(directory);
    }
    public static void main(String[] args) throws UnhandledCaseException, InterruptedException, IOException {
        AppRunner runner = new AppRunner();
        JCommander cmd = new JCommander(runner);
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            LOG.error("Argument error: " + ex.getMessage());
            cmd.usage();
        }
        try {
            runner.run();
        } catch (InterruptedException ex) {
            LOG.error("Error in running topology locally", ex);
        }
    }
}

