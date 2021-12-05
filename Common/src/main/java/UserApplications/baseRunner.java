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

import static System.Constants.Mac_Project_Path;
import static System.Constants.Node22_Project_Path;

public abstract class baseRunner {
    public static final Logger LOG= LoggerFactory.getLogger(baseRunner.class);
    protected static String CFG_PATH = null;
    @Parameter(names={"-a","--app"},description = "The application to be executed",required = false)
    //public String application = "GS_txn";
    public String application = "TP_txn";
    //public String application = "WordCount";
    @Parameter(names = {"-t", "--Brisk.topology-name"}, required = false, description = "The name of the Brisk.topology")
    public String topologyName;
    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = "";
    @Parameter(names = {"--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;
    @Parameter(names = {"--THz", "-THz"}, description = "target input Hz")
    public double THz =2000 ;
    @Parameter(names = {"--shapshot"}, description = "shapshot interval")
    public int checkpoint = 3;// default shapshot interval=n*TStream interval
    @Parameter(names = {"--batch_number_per_wm"}, description = "TStream interval")
    public int batch_number_per_wm = 1000;// default TStream interval.
    @Parameter(names = {"--timeslice"}, description = "time slice used in spout (ms)")
    public int timeSliceLengthMs = 100;//ms


    @Parameter(names = {"--measure"}, description = "measure enable")
    public boolean measure = false;

    @Parameter(names = {"--shared"}, description = "shared by multi producers")
    public boolean shared = true;
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

    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the Brisk.topology (local mode only)")
    public int runtimeInSeconds = 30;

    @Parameter(names = {"-bt"}, description = "fixed batch", required = false)
    public int batch = 1000;

    @Parameter(names = {"-tt"}, description = "parallelism", required = false)
    public int tthread = 1;

    @Parameter(names = {"-DataBase"}, description = "DataBase", required = false)
    public String DataBase= "in-memory";


    public  baseRunner() {
        if(OsUtils.isMac()){
            CFG_PATH = Mac_Project_Path+"/Common/src/main/resources/config/%s.properties";
            metric_path = "../TSP_PM_Result/metric_output/";
        }else{
            CFG_PATH = Node22_Project_Path+"/Common/src/main/resources/config/%s.properties";
            metric_path = System.getProperty("user.home").concat("/TSP_PM_Result/metric_output/");
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
        config.put("timeSliceLengthMs", timeSliceLengthMs);
        config.put("benchmark", benchmark);
        config.put("profile", profile);
        config.put("NAV", NAV);
        config.put("application",application);

        config.put("measure", measure);

        config.put("linked", linked);
        config.put("shared",shared);
        config.put("common",common);

        if (batch != -1) {
            config.put("batch", batch);
        }

        config.put("microbenchmark", microbenchmark);
        config.put("metrics.output", metric_path);

        config.put("runtimeInSeconds", runtimeInSeconds);
        config.put("DataBase",DataBase);
        config.put("Sequential_Binding",true);

    }
}
