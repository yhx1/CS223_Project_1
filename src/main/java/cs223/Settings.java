package cs223;

import java.sql.Connection;

public class Settings {

    public static int MPL = 10;

    public static int TIME_UNIT_SECS = 1440; // Do not change

    public static int ISOLATION_LEVEL = Connection.TRANSACTION_SERIALIZABLE;

    public static int INTERVAL_BETWEEN_TIME_UNIT = 1000; // Do not change

    public static boolean DO_NOT_GROUP_DATA_OPERATIONS = false;

    public final static String PREPROCESSED_DATA_URL = "preprocessed/"; //Do not change

    public static int TEST_RUNNING_TIME_SECS = 3;

    public static String SEMANTIC_DATASET_URL = "Resources/data/low_concurrency/semantic_observation_low_concurrency.sql";

    public static String OBSERVATION_DATASET_URL = "Resources/data/low_concurrency/observation_low_concurrency.sql";

    public static String METADATA_DATASET_URL = "Resources/data/low_concurrency/metadata.sql";

    public static String QUERY_DATA_URL = "Resources/queries/low_concurrency/queries.txt";

    public static boolean HIGH_CONCURRENCY = true;

    public static void switch_to_high_concurrency() {
        SEMANTIC_DATASET_URL = "Resources/data/high_concurrency/semantic_observation_high_concurrency.sql";
        OBSERVATION_DATASET_URL = "Resources/data/high_concurrency/observation_high_concurrency.sql";
        METADATA_DATASET_URL = "Resources/data/high_concurrency/metadata.sql";
        QUERY_DATA_URL = "Resources/queries/high_concurrency/queries.txt";
    }
}
