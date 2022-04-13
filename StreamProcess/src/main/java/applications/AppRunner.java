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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

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
            LOG.info(application+" running on the mac");
        }else {
            LOG.info(application+" running on the Node22");
        }
        // Loads the configuration file set by the user or the default
        // configuration
        // Prepared default configuration
        if (configStr==null){
            String cfg=String.format(CFG_PATH,application);
            String ftcfg=String.format(CFG_PATH,"FTConfig");
            Properties p = null;
            try {
                p = loadProperties(cfg);
                config.putAll(Configuration.fromProperties(p));
                p=loadProperties(ftcfg);
                config.putAll(Configuration.fromProperties(p));
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.metric_path=this.metric_path+application;
        }
        setConfiguration(config);
        //Set the fault tolerance mechanisms
        switch (config.getInt("FTOptions")){
            case 0:
                break;
            case 1:
                CONTROL.enable_wal=true;
                break;
            case 2:
                CONTROL.enable_snapshot=true;
                break;
            case 3:
                CONTROL.enable_clr=true;
                break;
        }
        //Set the parallel
        CONTROL.enable_parallel=CONTROL.enable_states_partition=config.getBoolean("isParallel");
        //Set the failure model
        switch (config.getInt("failureModel",0)){
            case 0:
                break;
            case 1:
                CONTROL.enable_transaction_abort=true;
                break;
            case 2:
                CONTROL.enable_states_lost=true;
                break;
            case 3:
                CONTROL.enable_transaction_abort=CONTROL.enable_states_lost=true;
                break;
        }
        CONTROL.Time_Control=config.getBoolean("enable_time_Interval");
        if(CONTROL.MAX_RECOVERY_TIME){
            CONTROL.failureTime= (int) (config.getInt("snapshot")*config.getInt("batch_number_per_wm")*config.getDouble("failureFrequency")-1);
        }else {
            if(OsUtils.isMac()){
                CONTROL.failureTime=(int)(config.getInt("TEST_NUM_EVENTS")*config.getDouble("failureFrequency"));
            }else {
                CONTROL.failureTime=(int)(config.getInt("NUM_EVENTS")*config.getDouble("failureFrequency"));
            }
        }
        //Set the application
        CONTROL.Arrival_Control=config.getBoolean("Arrival_Control");
        CONTROL.RATIO_OF_READ=config.getDouble("RATIO_OF_READ");
        CONTROL.NUM_ACCESSES=config.getInt("NUM_ACCESSES");
        CONTROL.NUM_ITEMS=config.getInt("NUM_ITEMS");
        CONTROL.NUM_EVENTS=config.getInt("NUM_EVENTS");
        CONTROL.ZIP_SKEW=config.getDouble("ZIP_SKEW");
        CONTROL.partition_num=config.getInt("partition_num");
        CONTROL.Exactly_Once=config.getBoolean("Exactly_Once");
    }

    private static double runTopologyLocally(Topology topology,Configuration conf) throws UnhandledCaseException, InterruptedException, IOException {
        TopologySubmitter submitter=new TopologySubmitter();
        final_topology=submitter.submitTopology(topology,conf);
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
            if(enable_wal||enable_snapshot||enable_clr){
                submitter.getOM().getEM().closeFTM();
            }
            submitter.getOM().getEM().exit();
            MeasureTools.showMeasureResult();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private void run() throws UnhandledCaseException, InterruptedException, IOException {
        LoadConfiguration();
        //set the MeasureTool
        MeasureTools tools=new MeasureTools(config.getInt("partition_num"),config.getInt("executor.threads"),config.getInt("FTOptions"));
        //Get the descriptor for thr given application
        AppDriver.AppDescriptor app=driver.getApp(application);
        // In case topology names is given, create one
        if (topologyName == null) {
            topologyName = application;
        }
        //Get the topology
        Topology topology=app.getTopology(topologyName,config);
        topology.addMachine(p);
        //Run the topology
        double rt=runTopologyLocally(topology,config);
        // decide the output path of metrics.
        String directory;
        String statsFolderPattern = OsUtils.osWrapperPostFix(config.getString("metrics.output"))
                + OsUtils.osWrapperPostFix("%s")
                + OsUtils.osWrapperPostFix("FTOption = %d")
                + OsUtils.osWrapperPostFix("Exactly_Once = %s")
                + OsUtils.osWrapperPostFix("Arrival_Control = %s")
                + OsUtils.osWrapperPostFix("failureTime = %d_targetHz = %d_NUM_EVENTS = %d_NUM_ITEMS = %d_ZIP_SKEW = %d_RATIO_OF_READ = %d_executor.threads = %d");
        directory = String.format(statsFolderPattern,
                config.getString("application"),
                config.getInt("FTOptions"),
                config.getInt("Exactly_Once"),
                config.getInt("Arrival_Control"),
                config.getInt("failureTime"),
                config.getInt("targetHz"),
                config.getInt("NUM_EVENTS"),
                config.getInt("ZIP_SKEW"),
                config.getInt("RATIO_OF_READ"),
                config.getInt("executor.threads"));
        MeasureTools.METRICS_REPORT(directory);
    }
    public static void main(String[] args) throws UnhandledCaseException, InterruptedException, IOException {
        AppRunner runner=new AppRunner();
        try {
            runner.run();
        } catch (InterruptedException ex) {
            LOG.error("Error in running topology locally", ex);
        }
    }
}

