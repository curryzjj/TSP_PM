package applications;

import System.Platform.Platform;
import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.baseRunner;
import applications.topology.WordCount;
import applications.topology.transactional.TP_txn;
import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.exception.UnhandledCaseException;
import streamprocess.components.topology.Topology;
import streamprocess.components.topology.TopologySubmitter;
import streamprocess.execution.runtime.threads.executorThread;

import java.io.IOException;
import java.util.Properties;
import java.util.Queue;

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
    }
    private void run() throws UnhandledCaseException, InterruptedException, IOException {
        //Get the running environment
        if(OsUtils.isMac()){
            LOG.info("Running on the mac");
        }else {
        }
        // Loads the configuration file set by the user or the default
        // configuration
        // Prepared default configuration
        if (configStr==null){
            String cfg=String.format(CFG_PATH,application);
            Properties p = null;
            try {
                p = loadProperties(cfg);
            } catch (IOException e) {
                e.printStackTrace();
            }
            config.putAll(Configuration.fromProperties(p));
            this.metric_path=this.metric_path+application;
        }
        setConfiguration(config);
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
        executorThread spoutThread = submitter.getOM().getEM().getSpoutThread();
        long start = System.currentTimeMillis();
        spoutThread.join((long) (3 * 1E3 * 60));//sync_ratio for sink thread to stop. Maximally sync_ratio for 10 mins
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
            submitter.getOM().getEM().closeCM();
            submitter.getOM().getEM().exit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
    public static void main(String[] args) throws UnhandledCaseException, InterruptedException, IOException {
        AppRunner runner=new AppRunner();
        runner.run();
    }
}

