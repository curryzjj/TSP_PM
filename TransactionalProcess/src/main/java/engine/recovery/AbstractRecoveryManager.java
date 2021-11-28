package engine.recovery;
import System.FileSystem.DataIO.DataInputViewStreamWrapper;
import System.FileSystem.ImplFSDataInputStream.LocalDataInputStream;
import System.FileSystem.Path;
import engine.Database;
import engine.Exception.DatabaseException;
import engine.log.LogRecord;
import engine.shapshot.SnapshotResult;
import engine.table.datatype.serialize.Deserialize;
import engine.table.tableRecords.TableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static engine.log.WALManager.writeExecutor;
import static utils.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static utils.TransactionalProcessConstants.FaultTolerance.END_OF_GLOBAL_LSN_MARK;

public class AbstractRecoveryManager {
    private static final Logger LOG= LoggerFactory.getLogger(AbstractRecoveryManager.class);
    private static class recoveryFromWalTask implements Callable<Long>{
        private Database db;
        private Path WALPath;
        private int rangeId;
        private long globalLSN;
        public recoveryFromWalTask(Database db, Path walPath, int rangeId, long globalLSN){
            this.db = db;
            WALPath = walPath;
            this.rangeId = rangeId;
            this.globalLSN = globalLSN;
        }
        @Override
        public Long call() throws Exception {
            return recoveryFromWAL(db,WALPath,rangeId,globalLSN);
        }
    }
    public static void recoveryFromSnapshot(Database db, SnapshotResult snapshotResult) throws IOException, ClassNotFoundException, DatabaseException {
        File snapshotFile=db.getFs().pathToFile(snapshotResult.getSnapshotPath());
        LocalDataInputStream inputStream=new LocalDataInputStream(snapshotFile);
        DataInputViewStreamWrapper inputViewStreamWrapper=new DataInputViewStreamWrapper(inputStream);
        int metaLength=inputViewStreamWrapper.readShort();
        List<String> tables=new ArrayList<>();
        List<Integer> backendType=new ArrayList<>();
        for(int i=0;i<metaLength;i++){
            tables.add(inputViewStreamWrapper.readUTF());
            backendType.add(inputViewStreamWrapper.readInt());
        }
        for(int i=0;i<metaLength;i++){
            inputStream.seek(snapshotResult.getKeyGroupRangeOffsets().getKeyGroupOffset(i));
            boolean isNewGroup=false;
            try{
                while (!isNewGroup){
                    int len=inputViewStreamWrapper.readInt();
                    if(Math.abs(len)==END_OF_KEY_GROUP_MARK){
                        isNewGroup=true;
                    }else {
                        String key=getKey(inputViewStreamWrapper,len);
                        TableRecord value=getValue(inputViewStreamWrapper);
                        db.InsertRecord(tables.get(i),value);
                    }
                }
            } catch (EOFException e){
                LOG.info("DB recovery from snapshot complete");
            }
        }
        inputStream.close();
        inputViewStreamWrapper.close();
    }
    public static long recoveryFromWAL(Database db,Path WALPath,int rangeId,long globalLSN) throws IOException, ClassNotFoundException, DatabaseException {
        Path filePath;
        File walFile;
        if(rangeId==-1){
            filePath=new Path(WALPath,"WAL");
            walFile=db.getFs().pathToFile(filePath);
        }else{
            filePath=new Path(WALPath,"WAL-"+rangeId);
            walFile=db.getFs().pathToFile(filePath);
        }
        LocalDataInputStream inputStream=new LocalDataInputStream(walFile);
        DataInputViewStreamWrapper inputViewStreamWrapper=new DataInputViewStreamWrapper(inputStream);
        List<LogRecord> commitLogRecords=new ArrayList<>();
        boolean isNewLSN=false;
        long theLastLSN = 0L;
        try{
            while(true){
                while (!isNewLSN){
                    int len=inputViewStreamWrapper.readInt();
                    if(len==END_OF_GLOBAL_LSN_MARK){
                        long gLSN=inputViewStreamWrapper.readLong();
                        if(gLSN<=globalLSN||globalLSN==-1){
                            theLastLSN=gLSN;
                            isNewLSN=true;
                        }
                    }else {
                        LogRecord value=getLogRecord(inputViewStreamWrapper,len);
                        commitLogRecords.add(value);
                    }
                }
                /** Only the complete commit log can be recovery*/
                if(isNewLSN){
                    for (Iterator<LogRecord> it = commitLogRecords.iterator(); it.hasNext(); ) {
                        LogRecord logRecord = it.next();
                        db.InsertRecord(logRecord.getTableName(),logRecord.getUpdateTableRecord());
                    }
                    commitLogRecords.clear();
                    isNewLSN=false;
                }
            }
        } catch (EOFException e){
            LOG.info("DB recovery from WAL-"+rangeId+" complete");
        }
        inputStream.close();
        inputViewStreamWrapper.close();
        return theLastLSN;
    }
    public static long parallelRecoveryFromWAL(Database db,Path WALPath,int rangeNum,long globalLSN) throws IOException, ClassNotFoundException, DatabaseException, InterruptedException {
        List<recoveryFromWalTask> callables=new ArrayList<>();
        for(int i=0;i<rangeNum;i++){
            callables.add(new recoveryFromWalTask(db,WALPath,i, globalLSN));
        }
        List<Future<Long>> futures=writeExecutor.invokeAll(callables);

        return 1L;
    }
    private static String getKey(DataInputViewStreamWrapper inputViewStreamWrapper) throws IOException {
        int len=inputViewStreamWrapper.readInt();
        byte[] re=new byte[len];
        inputViewStreamWrapper.readFully(re);
        String s=new String(re);
        return s;
    }
    private static String getKey(DataInputViewStreamWrapper inputViewStreamWrapper,int len) throws IOException {
        byte[] re=new byte[len];
        inputViewStreamWrapper.readFully(re);
        String s=new String(re);
        return s;
    }
    private static TableRecord getValue(DataInputViewStreamWrapper inputViewStreamWrapper) throws IOException, ClassNotFoundException {
        int len=inputViewStreamWrapper.readInt();
        byte[] re=new byte[len];
        inputViewStreamWrapper.readFully(re);
        TableRecord tableRecord= Deserialize.Deserialize2Object(re,TableRecord.class.getClassLoader());
        return tableRecord;
    }
    private static LogRecord getLogRecord(DataInputViewStreamWrapper inputViewStreamWrapper,int len) throws IOException, ClassNotFoundException {
        byte[] re=new byte[len];
        inputViewStreamWrapper.readFully(re);
        LogRecord logRecord=Deserialize.Deserialize2Object(re,LogRecord.class.getClassLoader());
        return logRecord;
    }
}
