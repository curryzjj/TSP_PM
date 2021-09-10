package streamprocess.components.operators.api;

import System.spout.helper.wrapper.StringStatesWrapper;
import org.slf4j.Logger;
import streamprocess.execution.runtime.tuple.Marker;

import java.io.FileNotFoundException;
import java.util.Scanner;

public abstract class AbstractSpout extends Operator {

    //the following are used for checkpoint(marker)
    protected int myiteration=0;//
    protected boolean success=true;
    protected long boardcast_time;
    //end


    AbstractSpout(Logger log) {
        super(log, true, -1, 1);
    }

    protected String getConfigKey(String template){ return String.format(template,configPrefix);}
    public abstract void nextTuple() throws InterruptedException;
    public void nextTuple_nonblocking() throws InterruptedException{ nextTuple();}

    //the following are used by the load_input
    private void construction(Scanner scanner, StringStatesWrapper wrapper){}
    private void spiltRead(String filename) throws FileNotFoundException{}
    private void read() throws  FileNotFoundException{};
    private void build(Scanner scanner){}
    private void openFile(String filename) throws FileNotFoundException{}
    protected void load_input(){}
    //end


    @Override
    public void callback(int callee, Marker marker) {}
}
