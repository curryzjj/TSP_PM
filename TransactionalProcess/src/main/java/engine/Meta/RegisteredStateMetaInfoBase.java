package engine.Meta;



import engine.shapshot.StateMetaInfoSnapshot;
import utils.TransactionalProcessConstants;

import javax.annotation.Nonnull;

public abstract class RegisteredStateMetaInfoBase {
    /** The name of the state */
    @Nonnull
    protected final String name;

    public RegisteredStateMetaInfoBase(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public abstract StateMetaInfoSnapshot snapshot();

    public static RegisteredStateMetaInfoBase fromMetaInfoSnapshot(
            @Nonnull StateMetaInfoSnapshot snapshot) {

        final TransactionalProcessConstants.BackendStateType backendStateType =
                snapshot.getBackendStateType();
        switch (backendStateType) {
            case KEY_VALUE:
                return new RegisteredKeyValueStateBackendMetaInfo(snapshot);
            default:
                throw new IllegalArgumentException(
                        "Unknown backend state type: " + backendStateType);
        }
    }
}
