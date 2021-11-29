package engine.shapshot;

public class CheckpointOptions {
    public int rangeNum;
    public int delta;
    public CheckpointOptions(int rangeNum,int delta){
        this.rangeNum=rangeNum;
        this.delta=delta;
    }
    public CheckpointOptions(){
        this.rangeNum=0;
    }
}
