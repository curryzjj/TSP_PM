package engine.log.LogStream;

import engine.log.LogResult;
import utils.CloseableRegistry.CloseableRegistry;

public interface UpdateLogWrite {
    LogResult get(CloseableRegistry logCloseableRegistry) throws Exception;
}
