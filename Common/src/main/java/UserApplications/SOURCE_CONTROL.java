package UserApplications;

import java.util.Map;
import java.util.concurrent.*;

/**
 * used in the TxnProcessing Engine to switch between two models
 */
public class SOURCE_CONTROL {
    public ConcurrentHashMap<Integer, Boolean> isArrived;
    private static SOURCE_CONTROL ourInstance = new SOURCE_CONTROL();
    private CyclicBarrier start_barrier;
    private CyclicBarrier end_barrier;
    private CyclicBarrier final_end_barrier;
    public static SOURCE_CONTROL getInstance(){
        return ourInstance;
    }
    public void config(int number_threads){
        start_barrier = new CyclicBarrier(number_threads);
        end_barrier = new CyclicBarrier(number_threads);
        final_end_barrier = new CyclicBarrier(number_threads);
        isArrived = new ConcurrentHashMap<>();
        for (int i = 0; i < number_threads; i++){
            isArrived.put( i, false);
        }
    }
    public void Wait_Start(int thread_Id){
        try{
            isArrived.put(thread_Id, true);
            start_barrier.await(2, TimeUnit.SECONDS);
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
    public void Wait_End(int thread_Id){
        try{
            end_barrier.await();
            Reset(thread_Id);
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void Reset(int thread_Id) {
        isArrived.put(thread_Id, false);
    }
    public void ResetAll() {
        isArrived.forEach((key, value) -> isArrived.put(key, false));
    }
}
