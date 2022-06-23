package applications.events.InputDataStore.ImplDataStore;

import applications.events.InputDataStore.InputStore;
import applications.events.TxnEvent;
import applications.events.lr.TollProcessingEvent;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class TPInputStore extends InputStore {
    private static final long serialVersionUID = -5133805365488855917L;
    @Override
    public void storeInput(List<TxnEvent> inputs) throws IOException {
        File file = new File(inputFile.concat(inputStorePaths.get(currentOffset)));
        FileWriter Fw = null;
        Fw = new FileWriter(file,true);
        BufferedWriter bw= new BufferedWriter(Fw);
        for (TxnEvent event:inputs){
            TollProcessingEvent tollProcessingEvent = (TollProcessingEvent) event;
            String str = tollProcessingEvent.toString();
            bw.write(str+"\n");
        }
        bw.flush();
        bw.close();
        Fw.close();
    }

    @Override
    public void close() {

    }
}
