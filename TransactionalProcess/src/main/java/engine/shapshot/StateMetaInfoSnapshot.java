package engine.shapshot;

import engine.table.RecordSchema;
import utils.TransactionalProcessConstants;

import javax.annotation.Nonnull;

public class StateMetaInfoSnapshot {
    /** The name of the state. */
    @Nonnull private final String name;
    @Nonnull private final TransactionalProcessConstants.BackendStateType backendStateType;
    @Nonnull private final RecordSchema recordSchema;

    /** Map of options (encoded as strings) for the state. */
//    @Nonnull private final Map<String, String> options;

    public StateMetaInfoSnapshot(@Nonnull String name,
                                 @Nonnull TransactionalProcessConstants.BackendStateType backendStateType,
                                 @Nonnull RecordSchema recordSchema
                               ) {
        this.name = name;
        this.backendStateType = backendStateType;
        this.recordSchema=recordSchema;
    }

    @Nonnull
    public TransactionalProcessConstants.BackendStateType getBackendStateType() {
        return backendStateType;
    }
    @Nonnull
    public String getName() {
        return name;
    }
    @Nonnull
    public RecordSchema getRecordSchema(){
        return recordSchema;
    }
}
