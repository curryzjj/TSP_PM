package engine.transaction.function;

public class DEC extends Function{
    public DEC(long delta) {
        this.delta_long = delta;
    }

    public DEC(long delta, Condition condition) {
        this.delta_long = delta;
    }
}
