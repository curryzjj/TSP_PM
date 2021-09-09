package streamprocess.execution.runtime.threads;

import System.Platform.Platform;
import System.util.Configuration;
import org.apache.log4j.Logger;
import streamprocess.components.topology.TopologyContext;
import streamprocess.execution.ExecutionNode;
import ch.usi.overseer.OverHpc;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;

public abstract class executorThread extends Thread {

    public final ExecutionNode executor;
    TopologyContext context;
    private boolean ready;
    int batch;


    protected executorThread(ExecutionNode e, Configuration conf, TopologyContext context,
                             long[] cpu, int node, CountDownLatch latch, OverHpc HPCMonotor,
                             HashMap<Integer, executorThread> threadMap){

        this.executor = e;
    }
    //get+set TopologyContext
    public TopologyContext getContext() {
        return context;
    }
    public void setContext(TopologyContext context) {
        this.context = context;
    }
    //end

    //bind Thread
    protected long[] sequential_binding(){ return null;}
    protected long[] binding(){ return null;}
    protected long[] rebinding(){ return null;}
    protected long[] rebinding_clean(){ return null;}
    //end

    //input+output queue
    public void initilize_queue(int executorID) {
        allocate_OutputQueue();
        assign_InputQueue();
    }
    private void allocate_OutputQueue() { }
    private void assign_InputQueue(){}
    //end

    //pause, restart, and migrate
    private void pause(){}
    private void pause_parent(){}
    private void restart(){}
    private void restart_parent(){}
    //The above is called by _migrate()
    private void _migrate(){}//called by the profile_routing
    public void migrate(){}
    //end

    //profile and routing(Exception->DatabaseException)
    protected abstract void _execute_noControl() throws InterruptedException, Exception, BrokenBarrierException;
    protected abstract void _execute() throws InterruptedException, Exception, BrokenBarrierException;
    protected abstract void _profile() throws InterruptedException, Exception, BrokenBarrierException;
    //The above is implemented by the bolt(spout)Thread and called by the below
    void profile_routing(Platform p) throws InterruptedException, Exception, BrokenBarrierException {}
    void routing() throws InterruptedException, Exception, BrokenBarrierException{}
    //end

    //public methods
    void Ready(Logger LOG){ ready=true; }
    public boolean isReady(){ return ready;}
    public int getExecutorID(){return executor.getExecutorID();}
    public String getOP(){return executor.getOP();}
    //end
}
