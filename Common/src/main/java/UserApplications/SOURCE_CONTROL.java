package UserApplications;

import java.util.concurrent.*;

/**
 * used in the TxnProcessing Engine to switch between two models
 */
public class SOURCE_CONTROL {
    private static SOURCE_CONTROL ourInstance = new SOURCE_CONTROL();
    private CyclicBarrier start_barrier;
    private CyclicBarrier end_barrier;
    public static SOURCE_CONTROL getInstance(){
        return ourInstance;
    }
    public void config(int number_threads){
        start_barrier = new CyclicBarrier(number_threads);
        end_barrier = new CyclicBarrier(number_threads);
    }
    public boolean Wait_Start(int thread_Id){
        try {
            start_barrier.await(2, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            System.out.println("TimeOut");
           return false;
        }
    }
    public void Wait_End(int thread_Id){
        try{
            end_barrier.await();
        } catch (BrokenBarrierException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
