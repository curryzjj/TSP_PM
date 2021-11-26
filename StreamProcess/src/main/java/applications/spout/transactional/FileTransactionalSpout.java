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
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import static System.Constants.Mac_Data_Path;
import static System.Constants.Node22_Data_Path;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public class FileTransactionalSpout extends TransactionalSpout {
    private static final Logger LOG= LoggerFactory.getLogger(FileTransactionalSpout.class);
    private InputDataGenerator inputDataGenerator;
    private Scanner scanner;
    private String Data_path;
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
        Data_path = "";
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
        Data_path = Data_path.concat(path);
        inputDataGenerator.initialize(Data_path,recordNum,NUM_SEGMENTS-1,zipSkew);
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
    public void nextTuple(int batch) throws InterruptedException {
        if(!startClock){
            this.clock.start();
            startClock=true;
        }
        if(!isCommit){
            this.registerRecovery();
        }
        List<String> inputData=inputDataGenerator.generateData(batch);
        if(inputData!=null){
            for (Iterator<String> it = inputData.iterator(); it.hasNext(); ) {
                String input = it.next();
                collector.emit(input.toCharArray(),bid);
                forward_checkpoint(this.taskId, bid, null,"marker");
                bid++;
            }
        }else{
            forward_checkpoint(this.taskId, bid, null,"marker");
            try {
                clock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
                if (taskId == graph.getSpout().getExecutorID()) {
                    LOG.info("Spout sent marker"+myiteration);
                    context.stop_running();
                }
        }
    }
    @Override
    public void setInputDataGenerator(InputDataGenerator inputDataGenerator) {
        this.inputDataGenerator=inputDataGenerator;
    }

    /**
     * Load data form input store, and replay the lost data
     * @param offset
     * @throws FileNotFoundException
     * @throws InterruptedException
     */
    @Override
    public void recoveryInput(long offset) throws FileNotFoundException, InterruptedException {
        long msg=offset;
        int lostdata=0;
        openFile(Data_path);
        while (offset!=0){
            scanner.nextLine();
            offset--;
            bid++;
        }
        LOG.info("The input data have been load to the offset "+msg);
        while (scanner.hasNextLine()){
            collector.emit(scanner.nextLine().toCharArray(),bid);
            bid++;
            lostdata++;
        }
        LOG.info("The number of lost data is "+lostdata);
        scanner.close();
    }
    private void openFile(String fileName) throws FileNotFoundException {
        scanner = new Scanner(new File(fileName), "UTF-8");
    }
}
