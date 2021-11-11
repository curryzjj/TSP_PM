package utils.CloseableRegistry;


import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.*;

public class CloseableRegistry extends AbstractCloseableRegistry<Closeable,Object> {
    private static final Object DUMMY = new Object();

    public CloseableRegistry() {
        super(new LinkedHashMap<>());
    }

    @Override
    protected void doRegister(
            @Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
        closeableMap.put(closeable, DUMMY);
    }

    @Override
    protected boolean doUnRegister(
            @Nonnull Closeable closeable, @Nonnull Map<Closeable, Object> closeableMap) {
        return closeableMap.remove(closeable) != null;
    }

    @Override
    protected Collection<Closeable> getReferencesToClose() {
        ArrayList<Closeable> closeablesToClose = new ArrayList<>(closeableToRef.keySet());
        Collections.reverse(closeablesToClose);
        return closeablesToClose;
    }
}
