package System.sink.helper;


public class ApplicationResult {
    long bid;
    Double[] results;
    public ApplicationResult(long bid, Double[] results){
        this.bid = bid;
        this.results = results;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(bid);
        s.append(";");
        for (double d:results){
            s.append(d);
            s.append(",");
        }
        return s.toString();
    }
}
