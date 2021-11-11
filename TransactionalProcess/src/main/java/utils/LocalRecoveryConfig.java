package utils;


import javax.annotation.Nonnull;

public class LocalRecoveryConfig {
    /** The local recovery mode. */
    private final boolean localRecoveryEnabled;

    /** Encapsulates the root directories and the subtask-specific path. */
    @Nonnull
    private final LocalRecoveryDirectoryProvider localStateDirectories;

    public LocalRecoveryConfig(
            boolean localRecoveryEnabled,
            @Nonnull LocalRecoveryDirectoryProvider directoryProvider) {
        this.localRecoveryEnabled = localRecoveryEnabled;
        this.localStateDirectories = directoryProvider;
    }

    public boolean isLocalRecoveryEnabled() {
        return localRecoveryEnabled;
    }

    @Nonnull
    public LocalRecoveryDirectoryProvider getLocalStateDirectoryProvider() {
        return localStateDirectories;
    }

}
