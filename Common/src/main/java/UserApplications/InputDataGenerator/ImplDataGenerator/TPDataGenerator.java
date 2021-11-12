package UserApplications.InputDataGenerator.ImplDataGenerator;

import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.Path;
import System.tools.ZipfGenerator;
import System.tools.randomNumberGenerator;
import UserApplications.InputDataGenerator.InputDataGenerator;

import java.io.*;
import java.sql.Timestamp;

public class TPDataGenerator extends InputDataGenerator {
    private OutputStream outputStream;
    private LocalFileSystem localFileSystem;
    private String dataPath;
    private int recordNum;
    private double skew;

    public void generateData() throws IOException {
        String parent=System.getProperty("user.home").concat("/hair-loss/app/benchmarks/");
        String child="/txt.txt";
        File file=new File(dataPath);
        FileWriter Fw=new FileWriter(file,true);
        ZipfGenerator zipfGenerator=new ZipfGenerator(1000,skew);
        for(int i=0;i<recordNum;i++){
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            String str=timestamp.getTime()+" "+ randomNumberGenerator.generateRandom(0,100)+" "+randomNumberGenerator.generateRandom(60,180)+
                    " "+randomNumberGenerator.generateRandom(0,4)+" "+randomNumberGenerator.generateRandom(0,4)+" "+ randomNumberGenerator.generateRandom(0,1)+
                    " "+zipfGenerator.next()+" "+randomNumberGenerator.generateRandom(0,100);
            BufferedWriter bw= new BufferedWriter(Fw);
            bw.write(str);
            bw.newLine();
            bw.flush();
        }
    }
}
