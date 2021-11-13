package streamprocess.components.topology;

import System.util.Configuration;
import engine.Database;
import engine.ImplDatabase.InMemeoryDatabase;
import engine.ImplDatabase.RocksDBDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import streamprocess.execution.Initialize.TableInitilizer;
import utils.SpinLock;

import static System.constants.BaseConstants.DBOptions.In_Memory;
import static System.constants.BaseConstants.DBOptions.RocksDB;

public abstract class TransactionalTopology extends BasicTopology{
    private static final Logger LOG= LoggerFactory.getLogger(TransactionalTopology.class);
    public Database db;
    protected TransactionalTopology(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        InitializeDB();
    }

    protected void InitializeDB() {
        //switch different kinds of DB
        switch (config.getInt("DBOptions",0)){
            case In_Memory:this.db=new InMemeoryDatabase();
            break;
            case RocksDB: this.db=new RocksDBDatabase(config);
            break;
        }
    }
    public abstract TableInitilizer createDB(SpinLock[] spinlock);
}
