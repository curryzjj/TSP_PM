package streamprocess.components.operators.api;

import System.constants.BaseConstants;
import System.spout.helper.wrapper.StringStatesWrapper;
import System.util.OsUtils;
import UserApplications.InputDataGenerator.InputDataGenerator;
import org.slf4j.Logger;
import streamprocess.execution.ExecutionGraph;
import streamprocess.execution.runtime.tuple.msgs.Marker;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;//通过 Scanner 类来获取用户的输入

import static System.Constants.Mac_Data_Path;
import static UserApplications.CONTROL.NUM_EVENTS;

public abstract class AbstractSpout extends Operator {

    //the following are used for shapshot(marker)
    protected int myiteration=0;//
    protected boolean success=true;
    protected long boardcast_time;
    //end
    protected ArrayList<char[]> array;
    //	String[] array_array;
    protected char[][] array_array;
    protected int counter = 0;
    protected int taskId;
    protected int cnt;
    protected int exe;
    protected ExecutionGraph graph;

    protected AbstractSpout(Logger log) {
        super(log, true, -1, 1);
    }

    protected String getConfigKey(String template){ return String.format(template,configPrefix);}//append "application name"
    public abstract void nextTuple() throws InterruptedException;
    public void nextTuple_nonblocking() throws InterruptedException{ nextTuple();}

    //the following are used by the load_input
    private void construction(Scanner scanner, StringStatesWrapper wrapper){//used by build
        String splitregex=",";
        String[] words=scanner.nextLine().split(splitregex);
        StringBuilder stringBuilder=new StringBuilder();
        for(String word:words){
            stringBuilder.append(word).append(wrapper.getTuple_states()).append(splitregex);
        }
        array.add(stringBuilder.toString().toCharArray());
    }
    private void spiltRead(String filename) throws FileNotFoundException{//used by the openfile
        int numSpout=this.getContext().getComponent(taskId).getNumTasks();
        int range=10/numSpout;//original file is split into 10 sub-files.
        int offset=this.taskId*range+1;
        String[] split=filename.split("\\.");
        for (int i = offset; i < offset + range; i++) {
            read(split[0], i, split[1]);
        }

        if (this.taskId == numSpout - 1) {//if this is the last executor of spout
            for (int i = offset + range; i <= 10; i++) {
                read(split[0], i, split[1]);
            }
        }
    }
    private void read(String prefix, int i, String postfix) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File((prefix + i) + "." + postfix), "UTF-8");
        build(scanner);
    }
    private void build(Scanner scanner){//used by openFile
        cnt = 100;
        if (config.getInt("batch") == -1) {
            while (scanner.hasNext()) {
                array.add(scanner.next().toCharArray());//for micro-benchmark only
            }
        } else {

            if (!config.getBoolean("microbenchmark")) {//normal case..
                if (OsUtils.isMac()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        array.add(scanner.nextLine().toCharArray());
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        array.add(scanner.nextLine().toCharArray()); //normal..
                    }
                }

            } else {
                int tuple_size = config.getInt("size_tuple");
                LOG.info("Additional tuple size to emit:" + tuple_size);
                StringStatesWrapper wrapper = new StringStatesWrapper(tuple_size);
                if (OsUtils.isWindows()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        construction(scanner, wrapper);
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        construction(scanner, wrapper);
                    }
                }
            }
        }
        scanner.close();
    }
    private void openFile(String filename) throws FileNotFoundException{//used by load_input
        boolean split;
        split =!OsUtils.isMac()&&config.getBoolean("split",true);
        if(split){
            spiltRead(filename);
        }else{
            Scanner scanner=new Scanner(new File(filename),"UTF-8");
            build(scanner);
        }
        array_array = array.toArray(new char[array.size()][]);
        counter = 0;
    }
    protected void load_input(){
        long start=System.nanoTime();
        String OS_prefix=null;
        String path;
        String Data_path = "";
        if(OsUtils.isWindows()){
            OS_prefix="win.";
        }else{
            OS_prefix="unix.";
        }
        if(OsUtils.isMac()){
            path=config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
            Data_path=Mac_Data_Path;
        }else{
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
        }
        String s = Data_path.concat(path);
        array=new ArrayList<>();
        try{
            openFile(s);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        long pid=OsUtils.getJVMID();
        LOG.info("JVM PID = "+pid);
        FileWriter fw;
        BufferedWriter writer=null;
        File file=new File(config.getString("metrics.output"));//can not find
        if (!file.mkdirs()) {
            LOG.warn("Not able to create metrics directories");
        }
        String sink_path = config.getString("metrics.output") + OsUtils.OS_wrapper("sink_threadId.txt");
        try {
            fw = new FileWriter(new File(sink_path));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            //String s_pid = String.valueOf(print_pid);
            writer.write(String.valueOf(pid));
            writer.flush();
            //writer.clean();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        int end_index = array_array.length * config.getInt("count_number", 1);
        LOG.info("spout:" + this.taskId + " elements:" + end_index);
        long end = System.nanoTime();
        LOG.info("spout prepare takes (ms):" + (end - start) / 1E6);
    }
    //end
    //createInput for FileSpout
    public void setInputDataGenerator(InputDataGenerator inputDataGenerator){}

    @Override
    public void callback(int callee, Marker marker) {
        status.callback_spout(callee,marker,executor);
    }
}
