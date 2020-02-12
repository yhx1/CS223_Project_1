package cs223;

import java.sql.Connection;

public class Settings {

    public static int MPL = 6;

    public static int TIME_UNIT_SECS = 1440;

    public static int ISOLATION_LEVEL = Connection.TRANSACTION_READ_COMMITTED;

    public static int INTERVAL_BETWEEN_TIME_UNIT = 1000;

    public final static String PREPROCESSED_DATA_URL = "preprocessed/";
}