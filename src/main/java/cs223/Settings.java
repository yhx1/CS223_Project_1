package cs223;

import java.sql.Connection;

public class Settings {

    public static int MPL = 100;

    public static int TIME_UNIT_SECS = 1440; // Do not change

    public static int ISOLATION_LEVEL = Connection.TRANSACTION_REPEATABLE_READ;

    public static int INTERVAL_BETWEEN_TIME_UNIT = 1000; // Do not change

    public static boolean DO_NOT_GROUP_DATA_OPERATIONS = true;

    public final static String PREPROCESSED_DATA_URL = "preprocessed/"; //Do not change
}
