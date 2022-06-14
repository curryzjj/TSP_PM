package streamprocess.execution.Initialize;

import System.constants.BaseConstants;
import System.util.Configuration;
import System.util.OsUtils;
import applications.events.InputDataGenerator.DataHolder;
import applications.events.InputDataGenerator.InputDataGenerator;
import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.TxnEvent;
import applications.events.gs.MicroEvent;
import applications.events.lr.TollProcessingEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;
import engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyContext;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Scanner;

import static System.Constants.Mac_Data_Path;
import static System.Constants.Node22_Data_Path;

public abstract class TableInitilizer {
    private static final Logger LOG= LoggerFactory.getLogger(TableInitilizer.class);
    protected final Database db;
    protected final double scale_factor;
    protected final double theta;
    protected final int partition_num;
    protected final Configuration config;
    protected String dataRootPath;
    protected InputDataGenerator dataGenerator;

    protected TableInitilizer(Database db, double scale_factor, double theta, int partition_num, Configuration config) {
        this.db = db;
        this.scale_factor = scale_factor;
        this.theta = theta;
        this.partition_num = partition_num;
        this.config = config;
        this.configurePath(config);
    }

    public abstract void creates_Table(Configuration config) throws IOException;
    public abstract void loadDB(int thread_id, TopologyContext context);
    public abstract void reloadDB(List<Integer> rangeId);
    public boolean Generate() throws IOException {
        String folder = dataRootPath;
        File file = new File(folder);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
            return false;
        }
        //file.mkdirs();
        dataGenerator.generateStream();//prepare input events
        LOG.info(String.format("Data Generator will dump data at %s.", dataRootPath));
        dataGenerator.dumpGeneratedDataToFile();//store benchmark
        LOG.info("Data Generation is done...");
        dataGenerator.cleanDataStructures();
        return true;
    }
    protected void Load() throws IOException {
        Scanner scanner = new Scanner(new File(dataRootPath),"UTF-8");
        while(scanner.hasNextLine()){
            TxnEvent event;
            String read = scanner.nextLine();
            String[] split = read.split(";");
            switch (split[4]){
                case "MicroEvent":
                    event = new MicroEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//key_array
                            Boolean.parseBoolean(split[6]),//flag
                            Long.parseLong(split[7]),//timestamp
                            Boolean.parseBoolean(split[8])//isAbort
                    );
                    break;
                case "BuyingEvent":
                    event = new BuyingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], //bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//key_array
                            split[6],//price_array
                            split[7]  ,//qty_array
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
                    );
                    break;
                case "AlertEvent":
                    event = new AlertEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], // bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7],//price_array
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
                    );
                    break;
                case "ToppingEvent":
                    event = new ToppingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], Integer.parseInt(split[1]), //pid
                            //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7] , //top_array
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
                    );
                    break;
                case "DepositEvent":
                    event = new DepositEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//getAccountId
                            split[6],//getBookEntryId
                            Integer.parseInt(split[7]),  //getAccountTransfer
                            Integer.parseInt(split[8]),  //getBookEntryTransfer
                            Long.parseLong(split[9]),
                            Boolean.parseBoolean(split[10])
                    );
                    break;
                case "TransactionEvent":
                    event = new TransactionEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//getSourceAccountId
                            split[6],//getSourceBookEntryId
                            split[7],//getTargetAccountId
                            split[8],//getTargetBookEntryId
                            Integer.parseInt(split[9]),  //getAccountTransfer
                            Integer.parseInt(split[10]),  //getBookEntryTransfer
                            Long.parseLong(split[11]),
                            Boolean.parseBoolean(split[12])
                    );
                    break;
                case "TollProcessEvent" :
                    event = new TollProcessingEvent(
                            Integer.parseInt(split[0]), //bid
                            Integer.parseInt(split[1]), //pid
                            split[2], //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],
                            Integer.parseInt(split[6]),
                            Integer.parseInt(split[7]),
                            Long.parseLong(split[8]),
                            Boolean.parseBoolean(split[9])
                    );
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + split[4]);
            }
            DataHolder.events.add(event);
        }
    }
    public void Prepare_input_event() throws IOException{
        Generate();
        Load();
    }

    /**
     * Control the input file path
     * ApplicationName + TotalEvents + NumItems + DependencyNum + PartitionNum + Skew + ReadRatio + AbortRatio + DependencyRatio
     * @param config
     */
    protected void configurePath(Configuration config) {
        MessageDigest digest;
        String fileName = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes;
            bytes = digest.digest(String.format("%s_%d_%d_%d_%d_%d_%f_%d_%d_%d",
                    config.getString("application"),
                    config.getInt("NUM_EVENTS"),
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESSES"),
                    config.getInt("partition_num_per_txn"),
                    config.getInt("partition_num"),
                    config.getDouble("ZIP_SKEW"),
                    config.getInt("RATIO_OF_READ"),
                    config.getInt("RATIO_OF_ABORT"),
                    config.getInt("RATIO_OF_DEPENDENCY")).getBytes(StandardCharsets.UTF_8));
            fileName = DatatypeConverter.printHexBinary(bytes).concat(".data");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String statsFolderPattern = OsUtils.osWrapperPostFix(config.getString("application")) + fileName;
        if(OsUtils.isMac()){
            this.dataRootPath = Mac_Data_Path.concat(statsFolderPattern) ;
        }else{
            this.dataRootPath = Node22_Data_Path.concat(statsFolderPattern);
        }
    }
}
