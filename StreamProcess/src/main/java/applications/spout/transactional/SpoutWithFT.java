package applications.spout.transactional;

import System.constants.BaseConstants;
import System.util.Configuration;
import System.util.OsUtils;
import applications.DataTypes.AbstractInputTuple;
import applications.DataTypes.PositionReport;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.TxnEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.faulttolerance.checkpoint.Status;
import streamprocess.components.operators.api.TransactionalSpoutFT;
import streamprocess.execution.ExecutionGraph;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import static System.Constants.Mac_Data_Path;
import static System.Constants.Node22_Data_Path;
import static System.constants.BaseConstants.BaseStream.DEFAULT_STREAM_ID;
import static UserApplications.CONTROL.*;
import static UserApplications.constants.TP_TxnConstants.Conf.NUM_SEGMENTS;

public class SpoutWithFT extends TransactionalSpoutFT {
    private static final Logger LOG= LoggerFactory.getLogger(SpoutWithFT.class);
    private InputDataGenerator inputDataGenerator;
    private Scanner scanner;
    private String Data_path;

    public SpoutWithFT(){
        super(LOG);
        this.scalable=false;
        status=new Status();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("SpoutWithFT initialize is being called");
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
            recordNum=config.getInt(BaseConstants.BaseConf.RECORD_NUM_TEST);
            this.exe=recordNum;
            zipSkew=config.getDouble(getConfigKey(BaseConstants.BaseConf.ZIPSKEW_TEST));
            Data_path=Mac_Data_Path;
        }else{
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
            recordNum=config.getInt(BaseConstants.BaseConf.RECORD_NUM);
            this.exe=recordNum;
            zipSkew=config.getDouble(getConfigKey(BaseConstants.BaseConf.ZIPSKEW_NUM));
            Data_path=Node22_Data_Path;
        }
        this.batch_number_per_wm=config.getInt("batch_number_per_wm");
        this.checkpoint_interval = config.getInt("snapshot");
        Data_path = Data_path.concat(path);
        inputDataGenerator.initialize(Data_path,recordNum,NUM_ITEMS-1,zipSkew,config);
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
        if(needReplay){
            this.registerRecovery();
        }
        while(replay&&batch!=0){
            AbstractInputTuple input=replayTuple();
            if(input!=null){
                collector.emit_single(DEFAULT_STREAM_ID,bid,input);
                bid++;
                lostData++;
                batch--;
                forward_marker(this.taskId, bid, null,"marker");
            }
        }
        List<AbstractInputTuple> inputData=inputDataGenerator.generateData(batch);
        if(inputData!=null){
            for (Iterator<AbstractInputTuple> it = inputData.iterator(); it.hasNext(); ) {
                PositionReport input = (PositionReport) it.next();
                collector.emit_single(DEFAULT_STREAM_ID,bid,input);
                bid++;
                forward_marker(this.taskId, bid, null,"marker");
            }
        }else{
            if(enable_wal||enable_snapshot){
                this.getContext().getFTM().spoutRegister(bid);
            }
            collector.create_marker_boardcast(boardcast_time, DEFAULT_STREAM_ID, bid, myiteration,"finish");
            try {
                clock.close();
                inputDataGenerator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("Spout sent marker "+myiteration);
            LOG.info("Spout sent snapshot "+checkpoint_counter);
            context.stop_running();
        }
    }

    @Override
    protected void loadReplay() throws FileNotFoundException {
        long msg=offset;
        bid=0;
        openFile(Data_path);
        while (offset!=0){
            scanner.nextLine();
            offset--;
            bid++;
        }
        LOG.info("The input data have been load to the offset "+msg);
    }

    @Override
    protected TxnEvent replayEvent() {
       return null;
    }
    protected AbstractInputTuple replayTuple() {
        if(scanner.hasNextLine()){
            AbstractInputTuple input;
            String read = scanner.nextLine();
            String[] token = read.split(";");
            return new PositionReport(
                     Long.parseLong(token[0]),//time
                    Integer.parseInt(token[1]),//vid
                    Integer.parseInt(token[2]), // speed
                    Integer.parseInt(token[3]), // xway
                    Short.parseShort(token[4]), // lane
                    Short.parseShort(token[5]), // direction
                    Short.parseShort(token[6]), // segment
                    Integer.parseInt(token[7]//position
                    ));
        }else{
            scanner.close();
            LOG.info("The number of lost data is "+lostData);
            replay=false;
            return null;
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
        this.needReplay =true;
        this.replay=true;
        this.offset=offset;
    }

    private void openFile(String fileName) throws FileNotFoundException {
        scanner = new Scanner(new File(fileName), "UTF-8");
    }
}
