import System.FileSystem.DataIO.DataOutputView;
import System.FileSystem.DataIO.DataOutputViewStreamWrapper;
import System.FileSystem.FSDataOutputStream;
import System.FileSystem.ImplFS.LocalFileSystem;
import System.FileSystem.ImplFSDataOutputStream.LocalDataOutputStream;
import System.FileSystem.Path;
import System.tools.FastZipfGenerator;
import System.tools.ZipfGenerator;
import System.tools.randomNumberGenerator;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Scanner;

public class LocalFSTest {
    public static void main(String[] args) throws IOException {
        String parent=System.getProperty("user.home").concat("/hair-loss/app/benchmarks/");
        String child="/txt.txt";
        Path testPath=new Path(parent,child);
        ArrayList<char[]> array = new ArrayList<>();
        char[][] array_array;
        File file=new File(System.getProperty("user.home").concat("/hair-loss/app/benchmarks/")+"txt1.txt");
        FileWriter Fw=new FileWriter(file,true);
        ZipfGenerator zipfGenerator=new ZipfGenerator(100,0.4);
        for(int i=0;i<10000;i++){
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            String str=timestamp.getTime()+" "+ randomNumberGenerator.generateRandom(0,100)+" "+randomNumberGenerator.generateRandom(60,180)+
                    " "+randomNumberGenerator.generateRandom(0,4)+" "+randomNumberGenerator.generateRandom(0,4)+" "+ randomNumberGenerator.generateRandom(0,1)+
                    " "+zipfGenerator.next()+" "+randomNumberGenerator.generateRandom(0,100);
            BufferedWriter bw= new BufferedWriter(Fw);
            bw.write(str);
            bw.newLine();
            bw.flush();
        }


        Scanner scanner=new Scanner(file,"UTF-8");
        while(scanner.hasNextLine()){
            array.add(scanner.nextLine().toCharArray());
        }

    }
    }
