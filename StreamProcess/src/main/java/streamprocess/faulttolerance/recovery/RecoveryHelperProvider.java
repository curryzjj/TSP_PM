package streamprocess.faulttolerance.recovery;

import System.FileSystem.ImplFSDataInputStream.LocalDataInputStream;
import engine.shapshot.SnapshotResult;
import engine.table.datatype.serialize.Deserialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Read the current file and get the RecoveryHelper
 */
public class RecoveryHelperProvider {
    private static final Logger LOG= LoggerFactory.getLogger(RecoveryHelperProvider.class);
    public static SnapshotResult getLastCommitSnapshotResult(File recoveryFile) throws IOException, ClassNotFoundException {
        List<SnapshotResult> SnapshotResults = new ArrayList<>();
        LocalDataInputStream localDataInputStream=new LocalDataInputStream(recoveryFile);
        DataInputStream inputStream=new DataInputStream(localDataInputStream);
        String s=inputStream.readUTF();
        LOG.info("Last Time: "+s);
        int len=0;
        try{
            while(true){
                len=inputStream.readInt();
                byte[] lastSnapResultBytes=new byte[len];
                inputStream.readFully(lastSnapResultBytes);
                SnapshotResult snapResult= Deserialize.Deserialize2Object(lastSnapResultBytes,SnapshotResult.class.getClassLoader());
                SnapshotResults.add(snapResult);
            }
        }catch (EOFException e){
            LOG.info("finish read the current.log");
        }finally {
            inputStream.close();
        }
        return SnapshotResults.get(SnapshotResults.size()-1);
    }
}
