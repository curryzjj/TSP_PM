package engine.shapshot.ShapshotResources;

public interface SnapshotResources {
    /** Cleans up the resources after the asynchronous part is done. */
    void release();
}
