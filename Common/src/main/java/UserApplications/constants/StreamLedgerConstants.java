package UserApplications.constants;

import System.constants.BaseConstants;

public interface StreamLedgerConstants extends BaseConstants {
    String PREFIX = "sltxn";

    interface Field {
        String TEXT = "text";
        String STATE = "state";
    }

    interface Conf extends BaseConf {
    }

    interface Component extends BaseComponent {
        String SL = "CTBolt";
        String EXECUTOR = "executor";

    }

    interface Constant {
        String ACCOUNT_ID_PREFIX = "";//ACCT-
        String BOOK_ENTRY_ID_PREFIX = "";//BOOK-
        long MAX_ACCOUNT_DEPOSIT = 1000;
        long MAX_BOOK_DEPOSIT = 1000;
        long MAX_ACCOUNT_TRANSFER = 10;
        long MAX_BOOK_TRANSFER = 10;
        long MAX_BALANCE = 10;
        long MIN_BALANCE = -1;
    }
}
