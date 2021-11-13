package UserApplications.InputDataGenerator;

import java.io.IOException;
import java.io.Serializable;

public abstract class InputDataGenerator implements Serializable {
    public abstract void generateData() throws IOException;
    public abstract void initialize(String dataPath,int recordNum,int range,double zipSkew);
}
