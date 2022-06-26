package streamprocess.execution.affinity;

import System.Platform.Platform;
import System.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class SequentialBinding {
    private static final Logger LOG= LoggerFactory.getLogger(SequentialBinding.class);
    static int socket = 1;
    static int cpu = 0;
    static int cpu_for_db;
    public static void SequentialBindingDB(){
        if(OsUtils.isMac()){
            cpu_for_db=6;
        }else{
            cpu_for_db=40;//config
        }
    }
    public static int next_cpu(){
        if(OsUtils.isMac()){
            if(cpu == 6){//skip first cpu--> it is reserved by OS.
                cpu = 0;
            }
            ArrayList[] mapping_node = Platform.getNodes(0);
            ArrayList<Integer> list = mapping_node[0];
            Integer core = list.get(cpu);
            cpu++;
            return core;
        }else {
            if (socket == 0 && cpu == 20) {
                throw new UnsupportedOperationException("out of cores!");
            }
            ArrayList[] mapping_node = Platform.getNodes(1);
            ArrayList<Integer> list = mapping_node[socket];
            Integer core = list.get(cpu);
            cpu ++;
            if (cpu == 20) {
                socket--;
                cpu = 0;
            }
            return core;
        }
    }
}
