package applications.spout.transactional;

import System.constants.BaseConstants;
import System.util.Configuration;
import System.util.OsUtils;
import UserApplications.InputDataGenerator.InputDataGenerator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.components.operators.api.TransactionalSpout;
import streamprocess.execution.ExecutionGraph;

import java.io.*;
import java.util.Scanner;

import static System.Constants.Mac_Data_Path;
import static System.Constants.Node22_Data_Path;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public class FileTransactionalSpout extends TransactionalSpout {
    private static final Logger LOG= LoggerFactory.getLogger(FileTransactionalSpout.class);
    private InputDataGenerator inputDataGenerator;
    private Scanner scanner;
    public FileTransactionalSpout(){
        super(LOG);
        this.scalable=false;
        status=new Status();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("FileTransactionSpout initialize is being called");
        cnt = 0;
        counter = 0;
        this.graph=graph;
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        String OS_prefix="";
        String path;
        String Data_path = "";
        int recordNum=0;
        double zipSkew=0;
        if(OsUtils.isWindows()){
            OS_prefix="win.";
        }else{
            OS_prefix="unix.";
        }
        if(OsUtils.isMac()){
            path=config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
            recordNum=config.getInt(getConfigKey(BaseConstants.BaseConf.RECORD_NUM_TEST));
            this.exe=recordNum;
            zipSkew=config.getDouble(getConfigKey(BaseConstants.BaseConf.ZIPSKEW_TEST));
            Data_path=Mac_Data_Path;
        }else{
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
            recordNum=config.getInt(getConfigKey(BaseConstants.BaseConf.RECORD_NUM));
            this.exe=recordNum;
            zipSkew=config.getDouble(getConfigKey(BaseConstants.BaseConf.ZIPSKEW_NUM));
            Data_path=Node22_Data_Path;
        }
        String s = Data_path.concat(path);
        inputDataGenerator.initialize(s,recordNum,NUM_SEGMENTS-1,zipSkew);
        LOG.info("Input Data Generation starts @" + DateTime.now());
        long start = System.nanoTime();
        try {
            inputDataGenerator.generateData();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        LOG.info("Input Data Generation takes:" + (end - start) / 1E6 + " ms");
        try {
            openFile(s);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        } else {
            return 1;
        }
    }
    @Override
    public void nextTuple() throws InterruptedException {
        if(!startClock){
            this.clock.start();
            startClock=true;
        }
        if(readLine()!=null){
            collector.emit(readLine(),bid);
            forward_checkpoint(this.taskId, bid, null,"marker");
            bid++;
        }else{
            forward_checkpoint(this.taskId, bid, null,"marker");
            try {
                clock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
                if (taskId == graph.getSpout().getExecutorID()) {
                    LOG.info("Thread:" + taskId + " is going to stop all threads sequentially");
                    LOG.info("Spout sent marker"+myiteration);
                    context.stop_running();
                }
        }
    }
    /**
     * Read one line from the currently open file. If there's only one file, each
     * instance of the spout will read only a portion of the file.
     *
     * @return The line
     */
    private char[] readLine() {
        if (scanner.hasNextLine())
            return scanner.nextLine().toCharArray();
        else
            return null;
    }
    /**
     * Opens the next file from the index. If there's multiple instances of the
     * spout, it will read only a portion of the files.
     */
    private void openFile(String fileName) throws FileNotFoundException {
        scanner = new Scanner(new File(fileName), "UTF-8");
    }
    @Override
    public void setInputDataGenerator(InputDataGenerator inputDataGenerator) {
        this.inputDataGenerator=inputDataGenerator;
    }
}
