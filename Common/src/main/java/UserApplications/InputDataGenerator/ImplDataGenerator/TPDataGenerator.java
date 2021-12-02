package UserApplications.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.ZipfGenerator;
import System.tools.randomNumberGenerator;
import UserApplications.InputDataGenerator.InputDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TPDataGenerator extends InputDataGenerator {
    private static final Logger LOG=LoggerFactory.getLogger(TPDataGenerator.class);
    private String dataPath;
    private int recordNum;
    private double zipSkew;
    private int range;
    private ZipfGenerator zipfGenerator;

    /**
     * Generate TP data in batch and store in the input store
     * @param batch
     * @return
     */
    public List<String> generateData(int batch){
        List<String> batch_input=new ArrayList<>();
        if(recordNum==0){
            return null;
        }
        File file=new File(dataPath);
        FileWriter Fw= null;
        try {
            Fw = new FileWriter(file,true);
            BufferedWriter bw= new BufferedWriter(Fw);
            for(int i=0;i<Math.min(recordNum,batch);i++){
                long timestamp = System.nanoTime();
                String str=timestamp+" "+ randomNumberGenerator.generateRandom(1,100)+" "+randomNumberGenerator.generateRandom(60,180)+
                        " "+randomNumberGenerator.generateRandom(1,4)+" "+randomNumberGenerator.generateRandom(1,4)+" "+ randomNumberGenerator.generateRandom(1,1)+
                        " "+zipfGenerator.next()+" "+randomNumberGenerator.generateRandom(1,100);
                batch_input.add(str);
                bw.write(str);
                bw.newLine();
                bw.flush();
            }
            bw.close();
            Fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        recordNum=recordNum-Math.min(recordNum,batch);
        return batch_input;
    }
    public void initialize(String dataPath,int recordNum,int range,double zipSkew){
        this.recordNum=recordNum;
        this.dataPath=dataPath;
        this.zipSkew=zipSkew;
        this.range=range;
        this.zipfGenerator=new ZipfGenerator(range, zipSkew);
    }

    @Override
    public void close() {
        LocalFileSystem fs=new LocalFileSystem();
        try {
            fs.delete(new Path(dataPath),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
