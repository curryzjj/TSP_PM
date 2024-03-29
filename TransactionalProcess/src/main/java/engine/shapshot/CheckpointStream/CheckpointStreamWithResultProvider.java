package engine.shapshot.CheckpointStream;

import System.FileSystem.Path;
import engine.shapshot.SnapshotResult;
import engine.table.keyGroup.KeyGroupRangeOffsets;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface CheckpointStreamWithResultProvider extends Closeable {
    Logger LOG= LoggerFactory.getLogger(CheckpointStreamFactory.class);
    default void close() throws IOException{
        getCheckpointOutputStream().close();
    }
    class PrimaryStreamOnly implements CheckpointStreamWithResultProvider{
        private final CheckpointStreamFactory.CheckpointStateOutputStream outputStream;

        public PrimaryStreamOnly(CheckpointStreamFactory.CheckpointStateOutputStream outputStream) {
            this.outputStream = outputStream;
        }
        @Override
        public CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream() {
            return outputStream;
        }

        @NotNull
        @Override
        public Path closeAndFinalizeCheckpointStreamResult() throws IOException {
            try{
                outputStream.flush();
                Path snapshotPath=outputStream.getStatePath();
                return snapshotPath;
            }catch (IOException e){
                throw new IOException();
            }
        }
    }
    CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream();
    /** Closes the stream and returns a snapshot result with the stream handle(s). */
    @Nonnull
    Path closeAndFinalizeCheckpointStreamResult() throws IOException;
    static CheckpointStreamWithResultProvider createSimpleStream(CheckpointStreamFactory checkpointStreamFactory) throws IOException {
        CheckpointStreamFactory.CheckpointStateOutputStream out=checkpointStreamFactory.createCheckpointStateOutputStream();
        return new PrimaryStreamOnly(out);
    }
    static List<CheckpointStreamWithResultProvider> createMultipleStream(CheckpointStreamFactory checkpointStreamFactory,int rangeNum) throws IOException {
        List<CheckpointStreamWithResultProvider> providers=new ArrayList<>();
        for (int i=0;i<rangeNum;i++){
            CheckpointStreamFactory.CheckpointStateOutputStream out=checkpointStreamFactory.createCheckpointStateOutputStream();
            providers.add(new PrimaryStreamOnly(out));
        }
        return providers;
    }

    /**
     * Create snapshot result to log this snapshot
     * @param snapshotPath
     * @param keyGroupRangeOffsets
     * @return
     */
    static SnapshotResult createSnapshotResult(Path snapshotPath, KeyGroupRangeOffsets keyGroupRangeOffsets,long timestamp,long checkpointId){
        return new SnapshotResult(snapshotPath,keyGroupRangeOffsets,timestamp,checkpointId);
    }
}
