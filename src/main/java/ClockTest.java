import java.io.Closeable;
import java.text.DateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class ClockTest {
    public static class Clock implements Closeable {
        long create_time;
        long gap;
        private int iteration = -100;//let it runs for a while...
        private Timer timer;
        public Clock(double checkpoint_interval) {
            gap = (long) (checkpoint_interval * (long) 1E3);//checkpoint_interval=0.1 -- 100ms by default.
            create_time = System.nanoTime();
            timer = new Timer();
        }
        public void start() {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                  System.out.println(iteration);
                    try {
                        Thread.sleep(5000);
                        System.out.println("a");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    iteration ++;
                }
            }, 1000, 5000);
        }
        public synchronized boolean tick(int myiteration) {
            return myiteration <= iteration;
        }
        @Override
        public void close() {
            timer.cancel();
            timer = null;
        }
    }
    public static void main(String[] args) throws InterruptedException {
        Clock c = new Clock(100);
        c.start();
    }
}
