package engine.shapshot;

public interface CheckpointListener {
    void notifyCheckpointComplete(long checkpointId) throws Exception;
    default void notifyCheckpointAborted(long checkpointId) throws Exception{};
}
