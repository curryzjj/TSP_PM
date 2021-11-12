package engine.Meta;

import engine.checkpoint.StateMetaInfoSnapshot;
import engine.table.RecordSchema;
import org.jetbrains.annotations.NotNull;
import utils.TransactionalProcessConstants;

public class RegisteredKeyValueStateBackendMetaInfo extends RegisteredStateMetaInfoBase {
    private final TransactionalProcessConstants.BackendStateType stateType;
    private final RecordSchema recordSchema;
    public RegisteredKeyValueStateBackendMetaInfo(StateMetaInfoSnapshot snapshot) {
        super(snapshot.getName());
        this.stateType=snapshot.getBackendStateType();
        this.recordSchema=snapshot.getRecordSchema();
    }
    public RegisteredKeyValueStateBackendMetaInfo(TransactionalProcessConstants.BackendStateType type, String name,RecordSchema recordSchema){
        super(name);
        this.stateType=type;
        this.recordSchema=recordSchema;
    }
    @NotNull
    @Override
    public StateMetaInfoSnapshot snapshot() {
        return computeSnapshot();
    }
    private StateMetaInfoSnapshot computeSnapshot(){
        return new StateMetaInfoSnapshot(name, TransactionalProcessConstants.BackendStateType.KEY_VALUE,recordSchema);
    }
}
