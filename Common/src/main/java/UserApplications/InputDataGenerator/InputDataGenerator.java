package UserApplications.InputDataGenerator;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public abstract class InputDataGenerator implements Serializable {
    /**
     * Generate data in batch and store in the input store
     * @param batch
     * @return
     */
    public abstract List<String> generateData(int batch);
    public abstract void initialize(String dataPath,int recordNum,int range,double zipSkew);
    public abstract void close();
}
