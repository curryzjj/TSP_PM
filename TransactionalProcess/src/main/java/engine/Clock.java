package engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class Clock implements Closeable {
    private static final Logger LOG= LoggerFactory.getLogger(Clock.class);
    long create_time;
    long gap;
    private int iteration=-10;
    private Timer timer;

    public Clock(double checkpoint_interval){
        gap=(long)(checkpoint_interval*(long) 1E3);
        LOG.info("Clock advance interval:"+checkpoint_interval);
        create_time=System.nanoTime();
        timer=new Timer();
    }
    public void start(){
      timer.scheduleAtFixedRate(new TimerTask() {
          @Override
          public void run() {
              iteration++;
          }
      },  gap, gap);//ms
    }
    public synchronized boolean tick(int myiteration){
        return myiteration <=iteration;
    }
    @Override
    public void close() throws IOException {
        timer.cancel();
        timer=null;
    }
}
