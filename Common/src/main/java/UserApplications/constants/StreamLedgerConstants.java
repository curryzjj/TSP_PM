package UserApplications.constants;

import System.constants.BaseConstants;

public interface StreamLedgerConstants extends BaseConstants {
    String PREFIX = "sltxn";

    interface Field {
        String TIME = "time";
        String TEXT = "text";
        String STATE = "state";
        String FLAG = "flag";
    }

    interface Conf extends BaseConf {
        String DEG_THREADS = "deposit.generator.threads";
        String TEG_THREADS = "txn.generator.threads";
        String DT_THREADS = "deposit.transaction.threads";
        String TT_THREADS = "txn.transaction.threads";
        String SL_THREADS = "ct.transaction.threads";
    }

    interface Component extends BaseComponent {
        String DEG = "depositGenerator";
        String TEG = "txnGenerator";
        String DT = "depositTxn";
        String TT = "transferTxn";
        String SL = "CTBolt";
        String EXECUTOR = "executor";

    }

    interface Constant {

        int NUM_ACCOUNTS = 100_000;
        int NUM_BOOK_ENTRIES = 100_000;
        String ACCOUNT_ID_PREFIX = "";//ACCT-
        String BOOK_ENTRY_ID_PREFIX = "";//BOOK-
        long MAX_ACCOUNT_DEPOSIT = 1000;
        long MAX_BOOK_DEPOSIT = 1000;
        long MAX_ACCOUNT_TRANSFER = 10;
        long MAX_BOOK_TRANSFER = 10;
        long MIN_BALANCE = 10;
        long MAX_BALANCE = -1;
    }
}
