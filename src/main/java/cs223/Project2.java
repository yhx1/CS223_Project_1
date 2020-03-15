package cs223;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

public class Project2 {
    public static int transactionSeqNum = 0;

    public static void createSchema() {

        for (int i = 1; i <= Settings.NUM_COHORTS; i++) {
            try{
                PostgresDataLoader.RunSQLByFile("Resources/schema/create.sql",
                        Settings.COHORT_DB_PREFIX+i,
                        Settings.P2_DB_USER,
                        Settings.P2_DB_PASSWORD);
                PostgresDataLoader.RunSQLByLine("cohort_log_create.sql",
                        Settings.COHORT_LOG_PREFIX+i,
                        Settings.P2_DB_USER,
                        Settings.P2_DB_PASSWORD);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //TODO: Create Schema for Coordinator Log DB
        try{
            PostgresDataLoader.RunSQLByLine("coord_log_create.sql",
                    Settings.COORD_LOG,
                    Settings.P2_DB_USER,
                    Settings.P2_DB_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void dropSchema() {
        for (int i = 1; i <= Settings.NUM_COHORTS; i++) {
            try{
                Connection con = null;
                Statement st = null;
                List<String> preparedTXs = new ArrayList<>();
                try {
                    con = DriverManager.getConnection(Settings.COHORT_DB_PREFIX + i, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
                    con.setAutoCommit(false);

                    st = con.createStatement();
                    ResultSet rs = st.executeQuery("SELECT gid FROM pg_prepared_xacts WHERE database = current_database();");
                    while (rs.next()) {
                        String TxID = rs.getString(1);
                        preparedTXs.add(TxID);
                    }

                    rs.close();

                    st.close();

                    con.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (!st.isClosed()) {
                            st.close();
                        }
                        con.close();
                    } catch (SQLException e) {
                        System.out.println("Failure to close connection.");
                    }
                }

                for (int j = 0; j < preparedTXs.size(); j++) {
                    try {
                        con = DriverManager.getConnection(Settings.COHORT_DB_PREFIX + i, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
                        con.setAutoCommit(true);

                        st = con.createStatement();
                        boolean rs = st.execute("ROLLBACK PREPARED \'"+preparedTXs.get(j)+"\';");
                        st.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (!st.isClosed()) {
                                st.close();
                            }
                            con.close();
                        } catch (SQLException e) {
                            System.out.println("Failure to close connection.");
                        }
                    }
                }

                PostgresDataLoader.RunSQLByLine("Resources/schema/drop.sql",
                        Settings.COHORT_DB_PREFIX+i,
                        Settings.P2_DB_USER,
                        Settings.P2_DB_PASSWORD);
                PostgresDataLoader.RunSQLByLine("cohort_log_drop.sql",
                        Settings.COHORT_LOG_PREFIX+i,
                        Settings.P2_DB_USER,
                        Settings.P2_DB_PASSWORD);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //TODO: Drop Schema for Coordinator Log DB
        try{
            PostgresDataLoader.RunSQLByLine("coord_log_drop.sql",
                    Settings.COORD_LOG,
                    Settings.P2_DB_USER,
                    Settings.P2_DB_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void loadMetadata() {
        for (int i = 1; i <= Settings.NUM_COHORTS; i++) {
            try{
                PostgresDataLoader.RunSQLByLine("Resources/data/low_concurrency/metadata.sql",
                        Settings.COHORT_DB_PREFIX+i,
                        Settings.P2_DB_USER,
                        Settings.P2_DB_PASSWORD);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void cleanUp() {
        dropSchema();
        createSchema();
        loadMetadata();
        System.out.println("Recreated Schemas and loaded Metadata.");
    }

    public static void ClientInputOnTick(MultiNodeEmulator nm, int time) throws Exception{
        String storageFilenamePrefix = Settings.PREPROCESSED_DATA_URL + time + "_";
        int segmentNumber = 0;
        File storageFile = new File(storageFilenamePrefix + segmentNumber);
        HashMap<String, ArrayList<String>> currentStatements;
        Random rand = new Random();

        currentStatements = new HashMap<String, ArrayList<String>>();

        // Read all segments of this time into one HashMap
        while (storageFile.exists()) {

            FileInputStream fis = new FileInputStream(storageFilenamePrefix + segmentNumber);
            ObjectInputStream ois = new ObjectInputStream(fis);
            HashMap<String, ArrayList<String>> temp = (HashMap<String, ArrayList<String>>) ois.readObject();
            ois.close();
            fis.close();

            Iterator<String> tempIterator = temp.keySet().iterator();

            while (tempIterator.hasNext()) {
                String sensorID = tempIterator.next();
                if (!currentStatements.containsKey(sensorID)) {
                    currentStatements.put(sensorID, temp.get(sensorID));
                } else {
                    currentStatements.get(sensorID).addAll(temp.get(sensorID));
                }
            }

            segmentNumber++;
            storageFile = new File(storageFilenamePrefix + segmentNumber);
        }

        Iterator<String> currentInsertsIterator = currentStatements.keySet().iterator();

        while (currentInsertsIterator.hasNext()) {
            String sensorID = currentInsertsIterator.next();
            long taskStartTime;

            /*
            if (Settings.DO_NOT_GROUP_DATA_OPERATIONS) {
                for (int j = 0; j < currentStatements.get(sensorID).size(); j++) {
                    taskStartTime = System.currentTimeMillis();
                    ArrayList temp = new ArrayList<String>();
                    temp.add(currentStatements.get(sensorID).get(j));
                    PostgresTransactionTask task = new PostgresTransactionTask(temp, metric, false, taskStartTime);
                    executor.execute(task);
                }
            }
            */
            {
                PreparedTransaction pt = new PreparedTransaction("T-"+transactionSeqNum);
                transactionSeqNum ++;
                List<String> temp = currentStatements.get(sensorID);

                for (int k = 0; k < temp.size(); k++) {
                    int cohort = Hasher.hash(temp.get(k));
                    //int cohort = rand.nextInt(3);

                    if (!pt.operations.containsKey(cohort+1)) {
                        pt.operations.put(cohort+1, new ArrayList<>());
                    }
                    pt.operations.get(cohort+1).add(temp.get(k));
                }

                nm.TransactionQueue.put(pt);
                //taskStartTime = System.currentTimeMillis();
                //PostgresTransactionTask task = new PostgresTransactionTask(currentStatements.get(sensorID), metric, false, taskStartTime);
                //executor.execute(task);
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        if (Settings.HIGH_CONCURRENCY) {
            Settings.switch_to_high_concurrency();
        }
        PostgresDataLoader.PreprocessInserts(Settings.OBSERVATION_DATASET_URL);

        cleanUp();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        // Create Emulator instance
        MultiNodeEmulator nm = new MultiNodeEmulator();

        // Set up all cohorts & coordinator
        {
            CohortTask ct1 = new CohortTask(nm, 1, nm.MessageQueues.get(1));
            executor.execute(ct1);
            CohortTimeoutCheckingTask ctct1 = new CohortTimeoutCheckingTask(nm, 1);
            executor.execute(ctct1);

            CohortTask ct2 = new CohortTask(nm, 2, nm.MessageQueues.get(2));
            executor.execute(ct2);
            CohortTimeoutCheckingTask ctct2 = new CohortTimeoutCheckingTask(nm, 2);
            executor.execute(ctct2);

            CohortTask ct3 = new CohortTask(nm, 3, nm.MessageQueues.get(3));
            executor.execute(ct3);
            CohortTimeoutCheckingTask ctct3 = new CohortTimeoutCheckingTask(nm, 3);
            executor.execute(ctct3);

            CoordinatorMessageHandlerTask cmht = new CoordinatorMessageHandlerTask(nm);
            executor.execute(cmht);
            CoordinatorTimeoutCheckingTask ctct = new CoordinatorTimeoutCheckingTask(nm);
            executor.execute(ctct);
            CoordinatorClientReceiverTask ccrt = new CoordinatorClientReceiverTask(nm);
            executor.execute(ccrt);
        }

        // Feed in dataset on intervals
        for (int time = 0; time < Settings.TEST_RUNNING_TIME_SECS; time++) {
            ClientInputOnTick(nm, time);
            Thread.sleep(Settings.INTERVAL_BETWEEN_TIME_UNIT);
        }

        executor.shutdownNow();
        Thread.sleep(1000);
        return;
    }
}
