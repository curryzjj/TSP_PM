package engine.checkpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public class StateMetaInfoSnapshot {
    /** The name of the state. */
    @Nonnull private final String name;

    @Nonnull private final BackendStateType backendStateType;

    /** Map of options (encoded as strings) for the state. */
//    @Nonnull private final Map<String, String> options;

    public StateMetaInfoSnapshot(@Nonnull String name,
                                 @Nonnull BackendStateType backendStateType
                               ) {
        this.name = name;
        this.backendStateType = backendStateType;
//        this.options = options;
    }

    /** Enum that defines the different types of state that live in TSP backends. */
    public enum BackendStateType {
        KEY_VALUE,
    }
    @Nonnull
    public BackendStateType getBackendStateType() {
        return backendStateType;
    }
    @Nonnull
    public String getName() {
        return name;
    }
//    @Nullable
//    public String getOption(@Nonnull String key) {
//        return options.get(key);
//    }
}
