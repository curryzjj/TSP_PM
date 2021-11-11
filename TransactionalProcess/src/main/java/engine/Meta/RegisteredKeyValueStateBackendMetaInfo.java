package engine.Meta;

import engine.checkpoint.StateMetaInfoSnapshot;
import org.jetbrains.annotations.NotNull;

public class RegisteredKeyValueStateBackendMetaInfo extends RegisteredStateMetaInfoBase {
    public RegisteredKeyValueStateBackendMetaInfo(StateMetaInfoSnapshot snapshot) {
        super(snapshot.getName());
    }

    @NotNull
    @Override
    public StateMetaInfoSnapshot snapshot() {
        return computeSnapshot();
    }
    private StateMetaInfoSnapshot computeSnapshot(){
        return new StateMetaInfoSnapshot(name, StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
    }
}
