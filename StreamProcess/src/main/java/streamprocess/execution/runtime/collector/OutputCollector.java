package streamprocess.execution.runtime.collector;

import System.util.OsUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.components.topology.TopologyComponent;
import streamprocess.components.topology.TopologyContext;
import streamprocess.controller.output.OutputController;
import streamprocess.execution.ExecutionNode;

import static System.Constants.DEFAULT_STREAM_ID;

/**
 * outputCollector is unique to each executor
 * emit the StreamValues by the OutputController
 */
public class OutputCollector<T> {
    private static final Logger LOG = LoggerFactory.getLogger(OutputCollector.class);
    public final OutputController sc;
    private final ExecutionNode executor;
    private final MetaGroup meta;
    private boolean no_wait=false;
    public OutputCollector(ExecutionNode executor, TopologyContext context){
        int taskId=context.getThisTaskIndex();
        this.executor=executor;
        this.sc=executor.getController();
        this.meta=new MetaGroup(taskId);
        for(TopologyComponent childrenOP:this.executor.getChildren().keySet()){
            this.meta.put(childrenOP,new Meta(taskId));
        }
        if (OsUtils.isMac()) {
            LogManager.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        } else {
            LogManager.getLogger(LOG.getName()).setLevel(Level.INFO);
        }
        OsUtils.configLOG(LOG);
    }
    public void emit(char[] data) throws InterruptedException{
        emit(DEFAULT_STREAM_ID,data);
    }
    public void emit_nowait(char[] data) throws InterruptedException{
        emit_nowait(DEFAULT_STREAM_ID,data);
    }
    private void emit_nowait(String streamId, char[] str) throws InterruptedException {
        if (executor.isLeafNode()) {
            return;
        }
        assert str != null && sc != null;
        sc.emitOnStream_nowait(meta, streamId, str);
    }
    private void emit(String streamId,char[] data) throws InterruptedException{
        assert data !=null && sc!=null;
        sc.emitOnStream(meta,streamId,data);
    }
    //force_emit
    public void force_emit(char[] values) throws InterruptedException {
        force_emit(DEFAULT_STREAM_ID, values);
    }
    public void force_emit(String streamId, char[] data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, streamId, data);
    }
    public void force_emit(long bid, Object... data) throws InterruptedException {
        assert data != null && sc != null;
        sc.force_emitOnStream(meta, DEFAULT_STREAM_ID, bid, data);
    }
}
