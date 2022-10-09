package applications.events;

import applications.events.SL.DepositEvent;
import applications.events.SL.TransactionEvent;
import applications.events.gs.MicroEvent;
import applications.events.lr.TollProcessingEvent;
import applications.events.ob.AlertEvent;
import applications.events.ob.BuyingEvent;
import applications.events.ob.ToppingEvent;

public class DeserializeEventHelper {
    public static TxnEvent deserializeEvent(String eventString) {
        TxnEvent event;
        String[] split = eventString.split(";");
        switch (split[4]){
            case "MicroEvent":
                event = new MicroEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//key_array
                        split[6],//values
                        Boolean.parseBoolean(split[7]),//flag
                        Long.parseLong(split[8]),//timestamp
                        Boolean.parseBoolean(split[9])//isAbort
                );
                break;
            case "BuyingEvent":
                event=new BuyingEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], //bid_array
                        Integer.parseInt(split[1]),//pid
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//key_array
                        split[6],//price_array
                        split[7]  ,//qty_array
                        Long.parseLong(split[8]),
                        Boolean.parseBoolean(split[9])
                );
                break;
            case "AlertEvent":
                event = new AlertEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], // bid_array
                        Integer.parseInt(split[1]),//pid
                        Integer.parseInt(split[3]),//num_of_partition
                        Integer.parseInt(split[5]), //num_access
                        split[6],//key_array
                        split[7],//price_array
                        Long.parseLong(split[8]),
                        Boolean.parseBoolean(split[9])
                );
                break;
            case "ToppingEvent":
                event = new ToppingEvent(
                        Integer.parseInt(split[0]), //bid
                        split[2], Integer.parseInt(split[1]), //pid
                        //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        Integer.parseInt(split[5]), //num_access
                        split[6],//key_array
                        split[7] , //top_array
                        Long.parseLong(split[8]),
                        Boolean.parseBoolean(split[9])
                );
                break;
            case "DepositEvent":
                event = new DepositEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//getAccountId
                        split[6],//getBookEntryId
                        Integer.parseInt(split[7]),  //getAccountTransfer
                        Integer.parseInt(split[8]),  //getBookEntryTransfer
                        Long.parseLong(split[9]),
                        Boolean.parseBoolean(split[10])
                );
                break;
            case "TransactionEvent":
                event = new TransactionEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],//getSourceAccountId
                        split[6],//getSourceBookEntryId
                        split[7],//getTargetAccountId
                        split[8],//getTargetBookEntryId
                        Integer.parseInt(split[9]),  //getAccountTransfer
                        Integer.parseInt(split[10]),  //getBookEntryTransfer
                        Long.parseLong(split[11]),
                        Boolean.parseBoolean(split[12])
                );
                break;
            case "TollProcessEvent" :
                event = new TollProcessingEvent(
                        Integer.parseInt(split[0]), //bid
                        Integer.parseInt(split[1]), //pid
                        split[2], //bid_array
                        Integer.parseInt(split[3]),//num_of_partition
                        split[5],
                        Integer.parseInt(split[6]),
                        Integer.parseInt(split[7]),
                        Long.parseLong(split[8]),
                        Boolean.parseBoolean(split[9])
                );
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + split[4]);
        }
        return event;
    }
}
