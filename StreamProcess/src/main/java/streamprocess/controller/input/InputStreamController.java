package streamprocess.controller.input;

import java.io.Serializable;

/**
 * for an executor (except spout's executor), there's a receive queue
 * for *each* upstream executor's *each* stream output (if subscribed).
 * streamId, SourceId -> queue
 */

public abstract class InputStreamController implements Serializable {

}
