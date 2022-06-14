package engine.Meta;


import System.FileSystem.DataIO.DataOutputView;
import engine.shapshot.StateMetaInfoSnapshot;

import java.io.IOException;

/**
 * Static factory that gives out writers and readers for different versions of {@link engine.shapshot.StateMetaInfoSnapshot}
 */
public class StateMetaInfoSnapshotReadersWriters {
    private StateMetaInfoSnapshotReadersWriters(){};
    /** Returns the writer for {@link StateMetaInfoSnapshot}. */
    public static CurrentWriterImpl getWriter(){return CurrentWriterImpl.INSTANCE;}
    public static class CurrentWriterImpl {
        private static final CurrentWriterImpl INSTANCE = new CurrentWriterImpl();
        public void writeStateMetaInfoSnapshot(StateMetaInfoSnapshot snapshot, DataOutputView outputView) throws IOException {
            outputView.writeUTF(snapshot.getName());
            //outputView.writeInt(snapshot.getBackendStateType().ordinal());
        }
    }
}
