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
    }
    private void run() throws UnhandledCaseException, InterruptedException, IOException {
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
                enable_wal=true;
                break;
            case 2:
                enable_snapshot=true;
                break;
        }
        //Set the parallel
        enable_parallel=enable_states_partition=config.getBoolean("isParallel");
        //Set the failure model
        switch (config.getInt("failureModel",0)){
            case 0:
                break;
            case 1:
                enable_transaction_abort=true;
                break;
            case 2:
                enable_states_lost=true;
                break;
            case 3:
                enable_transaction_abort=enable_states_lost=true;
                break;
        }
        if(OsUtils.isMac()){
            failureTime=(int)(config.getInt("TEST_NUM_EVENTS")*config.getDouble("failureTime"));
        }else {
            failureTime=(int)(config.getInt("NUM_EVENTS")*config.getDouble("failureTime"));
        }
        //Set the application
        Arrival_Control=config.getBoolean("Arrival_Control");
        RATIO_OF_READ=config.getDouble("RATIO_OF_READ");
        NUM_ACCESSES=config.getInt("NUM_ACCESSES");
        NUM_ITEMS=config.getInt("NUM_ITEMS");
        NUM_EVENTS=config.getInt("NUM_EVENTS");
        TEST_NUM_EVENTS=config.getInt("TEST_NUM_EVENTS");
        ZIP_SKEW=config.getDouble("ZIP_SKEW");
        partition_num=config.getInt("partition_num");
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
        //TODO:implement the wait after the shapshot
        Thread.sleep((long) (3 * 1E3 * 1));
        submitter.getOM().join();
        try {
            final_topology.db.close();
            if(enable_wal||enable_snapshot){
                submitter.getOM().getEM().closeFTM();
            }
            submitter.getOM().getEM().exit();
            MeasureTools.showMeasureResult();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
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

