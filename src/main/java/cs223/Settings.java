package cs223;

import java.sql.Connection;

public class Settings {

    public static int NUM_COHORTS = 3;

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

    public static boolean HIGH_CONCURRENCY = true;

    public static String COHORT_DB_PREFIX = "jdbc:postgresql://localhost:5432/cohort";
    public static String COHORT_LOG_PREFIX = "jdbc:postgresql://localhost:5432/cohortlog";
    public static String COORD_LOG = "jdbc:postgresql://localhost:5432/coordlog";
    public static String P2_DB_USER = "cs223p2";
    public static String P2_DB_PASSWORD = "cs223p2";

    public static int P2_PREPARE_TIMEOUT_MILLISECOND = 1000;
    public static int P2_DECISION_TIMEOUT_MILISECOND = 2000;
    public static int P2_ACK_TIMEOUT_MILISECOND = 1000;

    public static final int VOTE_IN_PROGRESS = 0;
    public static final int VOTE_ABORT = 1;
    public static final int VOTE_COMMIT = 2;


    public static void switch_to_high_concurrency() {
        SEMANTIC_DATASET_URL = "Resources/data/high_concurrency/semantic_observation_high_concurrency.sql";
        OBSERVATION_DATASET_URL = "Resources/data/high_concurrency/observation_high_concurrency.sql";
        METADATA_DATASET_URL = "Resources/data/high_concurrency/metadata.sql";
    }
}
