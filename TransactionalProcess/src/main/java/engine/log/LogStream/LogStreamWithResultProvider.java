package engine.log.LogStream;

import System.FileSystem.Path;
import engine.log.LogResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface LogStreamWithResultProvider extends Closeable {
    Logger LOG= LoggerFactory.getLogger(LogStreamWithResultProvider.class);
    LogStreamFactory.LogOutputStream getLogOutputStream();
    default void close() throws IOException {
        getLogOutputStream().close();
    }
    class PrimaryStreamOnly implements LogStreamWithResultProvider{
        private final LogStreamFactory.LogOutputStream outputStream;

        public PrimaryStreamOnly(LogStreamFactory.LogOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public LogStreamFactory.LogOutputStream getLogOutputStream() {
            return outputStream;
        }

        @Override
        public Path closeAndFinalizeLogCommitStreamResult() throws IOException {
            try{
                outputStream.flush();
                Path logPath=outputStream.getLogPath();
                //outputStream.close();
                return logPath;
            }catch (IOException e){
                throw new IOException();
            }
        }
    }

    Path closeAndFinalizeLogCommitStreamResult() throws IOException;
    static LogStreamWithResultProvider createSimpleStream(LogStreamFactory LogStreamFactory) throws IOException{
        LogStreamFactory.LogOutputStream out= LogStreamFactory.createLogOutputStream(-1);
        return new PrimaryStreamOnly(out);
    }
    static List<LogStreamWithResultProvider> createMultipleStream(LogStreamFactory LogStreamFactory,int rangeNum)throws IOException{
        List<LogStreamWithResultProvider> providers=new ArrayList<>();
        for (int i=0;i<rangeNum;i++){
            LogStreamFactory.LogOutputStream out= LogStreamFactory.createLogOutputStream(i);
            providers.add(new PrimaryStreamOnly(out));
        }
        return providers;
    }
    /**
     * Create logResult to log this group DB update
     * @return
     */
    static LogResult createLogResult(){
        return new LogResult();
    }
}
