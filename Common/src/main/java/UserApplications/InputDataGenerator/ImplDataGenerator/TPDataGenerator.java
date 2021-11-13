package UserApplications.InputDataGenerator.ImplDataGenerator;

import System.tools.ZipfGenerator;
import System.tools.randomNumberGenerator;
import UserApplications.InputDataGenerator.InputDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Timestamp;

public class TPDataGenerator extends InputDataGenerator {
    private static final Logger LOG=LoggerFactory.getLogger(TPDataGenerator.class);
    private String dataPath;
    private int recordNum;
    private double zipSkew;
    private int range;

    public void generateData() throws IOException {
        File file=new File(dataPath);
        if(file.exists()){
            LOG.info("Input data is ready");
            return;
        }
        FileWriter Fw=new FileWriter(file,true);
        ZipfGenerator zipfGenerator=new ZipfGenerator(range, zipSkew);
        BufferedWriter bw= new BufferedWriter(Fw);
        for(int i=0;i<recordNum;i++){
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            String str=timestamp.getTime()+" "+ randomNumberGenerator.generateRandom(0,100)+" "+randomNumberGenerator.generateRandom(60,180)+
                    " "+randomNumberGenerator.generateRandom(0,4)+" "+randomNumberGenerator.generateRandom(0,4)+" "+ randomNumberGenerator.generateRandom(0,1)+
                    " "+zipfGenerator.next()+" "+randomNumberGenerator.generateRandom(0,100);
            bw.write(str);
            bw.newLine();
            bw.flush();
        }
        bw.close();
        Fw.close();
    }
    public void initialize(String dataPath,int recordNum,int range,double zipSkew){
        this.recordNum=recordNum;
        this.dataPath=dataPath;
        this.zipSkew=zipSkew;
        this.range=range;
    }
}
