package UserApplications;

import java.util.HashMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * used in the TxnProcessing Engine to switch between two models
 */
public class SOURCE_CONTROL {
    public long counter = 0;
    private static SOURCE_CONTROL ourInstance = new SOURCE_CONTROL();
    private CyclicBarrier start_barrier;
    private CyclicBarrier end_barrier;
    private CyclicBarrier final_end_barrier;
    private HashMap<Integer,Integer> iteration;
    public static SOURCE_CONTROL getInstance(){
        return ourInstance;
    }
    public void config(int number_threads){
        start_barrier = new CyclicBarrier(number_threads);
        end_barrier = new CyclicBarrier(number_threads);
        final_end_barrier = new CyclicBarrier(number_threads);
        iteration = new HashMap<>();
        for (int i=0;i<number_threads;i++){
            iteration.put(i,0);
        }
    }
    public void Wait_Start(int thread_Id){
        try{
            start_barrier.await();
            counter ++;
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void Wait_End(int thread_Id){
        try{
            end_barrier.await();
            if (thread_Id == 0) {
                ResetCount();
            }
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void ResetCount() {
        counter = 0;
    }
}
