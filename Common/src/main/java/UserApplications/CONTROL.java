package UserApplications;

import java.util.SplittableRandom;
import java.util.Timer;

public class CONTROL {
    //application related.
    public static boolean Arrival_Control=false;
    public static int NUM_EVENTS = 40000000; //different input events.. TODO: It must be kept small as GC pressure increases rapidly. Fix this in future work.
    public static int NUM_ACCESSES=5;//10 as default setting. 2 for short transaction, 10 for long transaction.? --> this is the setting used in YingJun's work. 16 is the default value_list used in 1000core machine.
    public static int NUM_ITEMS = 10000;//1. 1_000_000; 2. ? ; 3. 1_000  //1_000_000 YCSB has 16 million records, Ledger use 200 million records.
    public static double RATIO_OF_READ=0.75;
    public static double ZIP_SKEW=0.4;
    public static boolean Time_Control=false;

    //combo optimization
    public static boolean enable_app_combo = false;//compose all operators into one.
    public static int combo_bid_size = 1;//reduce conflict. NOT applicable to LAL, LWM and PAT (must set to one).


    //order related.

    boolean enable_force_ordering = true;

    //db related.
    public static boolean enable_shared_state = true;//this is for transactional state mgmt.
    /**Configure in the appRunner**/
    // fault tolerance related.
    public static boolean enable_snapshot =false;
    public static boolean enable_wal = false;
    public static boolean enable_parallel=false;
    public static boolean enable_clr=false;
    public static boolean enable_states_partition = false;//must be enabled for parallel snapshot
    public static int partition_num=1;
    public static boolean enable_transaction_abort=false;
    public static boolean enable_states_lost=false;
    public static int failureTime=0;
    public static boolean MAX_RECOVERY_TIME=true;
    public static boolean Exactly_Once=true;
    //fault tolerance relax
    public static boolean enable_upstreamBackup=true;
    //latency related.
    public static boolean enable_latency_measurement = true;//

    //boolean enable_admission_control = enable_latency_measurement;//only enable for TStream

    //profile related.
    public static boolean  enable_measure = true;//enable this only when we want to test for breakdown.

    public static boolean enable_debug = true;//some critical debug section.

    //engine related.
    public static boolean enable_engine = true;//1. enable TP_engine. Always enabled. There's no meaning if we disable engine for T-Stream.
    public static boolean enable_numa_placement = true;//thread placement. always on.

    //used for NUMA-aware partition engine
    public static boolean enable_work_partition = true; // 2. this is a sub-option, only useful when engine is enabled.
    public static int island = -1;//-1 stands for one engine per core; -2 stands for one engine per socket.

    public static int CORE_PER_SOCKET = 2;//configure this for NUMA placement please.
    public static int NUM_OF_SOCKETS = 1;//configure this for NUMA placement please.

    //single engine with work-stealing.
    public static boolean enable_work_stealing = true; // won't affect is island=-1 under partition.


//    boolean enable_pushdown = false;//enabled by default.
    public static SplittableRandom rnd = new SplittableRandom(1234);
    public static  Timer timer =new Timer();


}
