package engine.shapshot;

public class CheckpointOptions {
    public int partitionNum;
    public int delta;
    public CheckpointOptions(int partitionNum, int delta){
        this.partitionNum = partitionNum;
        this.delta = delta;
    }
    public CheckpointOptions(){
        this.partitionNum =0;
    }
}
