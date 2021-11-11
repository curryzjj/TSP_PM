package engine.transaction.function;

public class AVG extends Function{
    public AVG(Integer delta) {
        this.delta_double = delta;
    }
    double getAVG(Integer latestAvg){
        double lav=0;
        if (latestAvg == 0) {//not initialized
            lav =this.delta_double;
        } else{
            lav = (latestAvg + this.delta_double) / 2;
        }
        return lav;
    }
}
