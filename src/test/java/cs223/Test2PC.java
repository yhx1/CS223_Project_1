package cs223;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Test2PC {

    //@Test
    public void testCreateSchema() {

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
                Assert.fail();
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
            Assert.fail();
        }

    }

    //@Test
    public void testDropSchema() {
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
                Assert.fail();
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
            Assert.fail();
        }
    }

    //@Test
    public void testLoadMetadata() {
        for (int i = 1; i <= Settings.NUM_COHORTS; i++) {
            try{
                PostgresDataLoader.RunSQLByLine("Resources/data/low_concurrency/metadata.sql",
                        Settings.COHORT_DB_PREFIX+i,
                        Settings.P2_DB_USER,
                        Settings.P2_DB_PASSWORD);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }

    @Before
    public void cleanUp() {
        testDropSchema();
        testCreateSchema();
        testLoadMetadata();
        System.out.println("Recreated Schemas and loaded Metadata.");
    }


    @Test
    public void test2PCSingleCohort() throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        CohortTask ct1 = new CohortTask(nm, 1, nm.MessageQueues.get(1));
        executor.execute(ct1);
        CohortTimeoutCheckingTask ctct1 = new CohortTimeoutCheckingTask(nm, 1);
        executor.execute(ctct1);

        CoordinatorMessageHandlerTask cmht = new CoordinatorMessageHandlerTask(nm);
        executor.execute(cmht);
        CoordinatorTimeoutCheckingTask ctct = new CoordinatorTimeoutCheckingTask(nm);
        executor.execute(ctct);

        CoordinatorClientReceiverTask ccrt = new CoordinatorClientReceiverTask(nm);
        executor.execute(ccrt);

        PreparedTransaction pt = new PreparedTransaction("T-1");

        List<String> cohort1ops = new ArrayList<>();
        cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
        cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
        cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
        pt.operations.put(1, cohort1ops);

        try {
            nm.TransactionQueue.put(pt);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }

    @Test
    public void test2PCMultiCohortWithAbort() throws InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
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


        PreparedTransaction pt1 = new PreparedTransaction("T-1");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
            pt1.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('b84af49f-5740-4e18-9912-33a077714c3d', 98, '2017-11-08 00:00:00', '79be1f14_c765_4cba_9420_35c0b78185b9');");
            pt1.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('05a164f5-cd44-44d6-a572-c827f56ea132', 76, '2017-11-08 00:00:00', 'fb4d44b2_4d81_4e65_a5f7_21a52319c197');");
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('e2aa766d-f351-4504-b71a-512538bed910', 46, '2017-11-08 00:00:00', '4777eb44_178b_406c_9b49_9adf3f20f6e9');");
            pt1.operations.put(3, cohort3ops);
        }

        PreparedTransaction pt2 = new PreparedTransaction("T-2");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('b9002eb2-1b1f-4917-8e7a-b9cf774166ee', 48, '2017-11-08 00:00:00', '44fbea6d_753f_4b6e_b9f5_9bf5f0cf6538');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('32e1609f-5973-41b4-838e-e1ff21590729', 30, '2017-11-08 00:00:00', '4149af1a_1f8c_44d2_b697_5f6b6a5ee496');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('f7f314dd-8776-4cbc-86eb-9494a54892b4', 30, '2017-11-08 00:00:00', '64542ab7_9599_4e77_845b_4391e812716c');");
            pt2.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('ff4d38e2-b6e8-4b6d-ba60-858f8b373f57', 4, '2017-11-09 00:27:00', 'thermometer3');");
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('3ec7937a-6a90-4c66-af88-ca706fc2f02b', 46, '2017-11-08 00:00:00', 'ce7285b9_c0c5_4945_9057_1ae84d92ed37');");
            pt2.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('ce0633d5-ef95-485a-badd-fe9200552118', 33, '2017-11-08 00:00:00', 'thermometer8');");
            pt2.operations.put(3, cohort3ops);
        }


        try {
            nm.TransactionQueue.put(pt1);
            nm.TransactionQueue.put(pt2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }

    @Test
    public void testAtomicityCohortDownBeforePrepareLog() throws InterruptedException{
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        nm.CrashEmulationConfigs.get(1).COHORT_CRASH_BEFORE_LOG_PREPARE = true;

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


        PreparedTransaction pt1 = new PreparedTransaction("T-1");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            //cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
            pt1.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('b84af49f-5740-4e18-9912-33a077714c3d', 98, '2017-11-08 00:00:00', '79be1f14_c765_4cba_9420_35c0b78185b9');");
            pt1.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('05a164f5-cd44-44d6-a572-c827f56ea132', 76, '2017-11-08 00:00:00', 'fb4d44b2_4d81_4e65_a5f7_21a52319c197');");
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('e2aa766d-f351-4504-b71a-512538bed910', 46, '2017-11-08 00:00:00', '4777eb44_178b_406c_9b49_9adf3f20f6e9');");
            pt1.operations.put(3, cohort3ops);
        }

        try {
            nm.TransactionQueue.put(pt1);
            Thread.sleep(1000);
            nm.CrashEmulationConfigs.get(1).reset();
            nm.TransactionTimestamps.get(1).clear();
            nm.MessageQueues.get(1).clear();
            ct1 = new CohortTask(nm, 1, nm.MessageQueues.get(1));
            ctct1 = new CohortTimeoutCheckingTask(nm, 1);
            executor.execute(ct1);
            executor.execute(ctct1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }

    @Test
    public void testAtomicityCohortDownBeforeVote() throws InterruptedException{
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        nm.CrashEmulationConfigs.get(1).COHORT_CRASH_BEFORE_VOTE = true;

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


        PreparedTransaction pt1 = new PreparedTransaction("T-1");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            //cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
            pt1.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('b84af49f-5740-4e18-9912-33a077714c3d', 98, '2017-11-08 00:00:00', '79be1f14_c765_4cba_9420_35c0b78185b9');");
            pt1.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('05a164f5-cd44-44d6-a572-c827f56ea132', 76, '2017-11-08 00:00:00', 'fb4d44b2_4d81_4e65_a5f7_21a52319c197');");
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('e2aa766d-f351-4504-b71a-512538bed910', 46, '2017-11-08 00:00:00', '4777eb44_178b_406c_9b49_9adf3f20f6e9');");
            pt1.operations.put(3, cohort3ops);
        }

        try {
            nm.TransactionQueue.put(pt1);
            Thread.sleep(2000);
            nm.CrashEmulationConfigs.get(1).reset();
            nm.TransactionTimestamps.get(1).clear();
            nm.MessageQueues.get(1).clear();
            ct1 = new CohortTask(nm, 1, nm.MessageQueues.get(1));
            ctct1 = new CohortTimeoutCheckingTask(nm, 1);
            executor.execute(ct1);
            executor.execute(ctct1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }

    @Test
    public void testAtomicityCohortDownAfterVoteYesAndRecovery() throws InterruptedException{
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        nm.CrashEmulationConfigs.get(1).COHORT_CRASH_AFTER_VOTE= true;

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


        PreparedTransaction pt1 = new PreparedTransaction("T-1");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            //cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
            pt1.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('b84af49f-5740-4e18-9912-33a077714c3d', 98, '2017-11-08 00:00:00', '79be1f14_c765_4cba_9420_35c0b78185b9');");
            pt1.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('05a164f5-cd44-44d6-a572-c827f56ea132', 76, '2017-11-08 00:00:00', 'fb4d44b2_4d81_4e65_a5f7_21a52319c197');");
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('e2aa766d-f351-4504-b71a-512538bed910', 46, '2017-11-08 00:00:00', '4777eb44_178b_406c_9b49_9adf3f20f6e9');");
            pt1.operations.put(3, cohort3ops);
        }

        try {
            nm.TransactionQueue.put(pt1);
            Thread.sleep(5000);
            nm.CrashEmulationConfigs.get(1).reset();
            nm.TransactionTimestamps.get(1).clear();
            nm.MessageQueues.get(1).clear();
            ct1 = new CohortTask(nm, 1, nm.MessageQueues.get(1));
            ctct1 = new CohortTimeoutCheckingTask(nm, 1);
            executor.execute(ct1);
            executor.execute(ctct1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }

    @Test
    public void testAtomicityCohortDownAfterVoteNoAndRecovery() throws InterruptedException{
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        nm.CrashEmulationConfigs.get(1).COHORT_CRASH_AFTER_VOTE = true;

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


        PreparedTransaction pt1 = new PreparedTransaction("T-1");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
            pt1.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('b84af49f-5740-4e18-9912-33a077714c3d', 98, '2017-11-08 00:00:00', '79be1f14_c765_4cba_9420_35c0b78185b9');");
            pt1.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('05a164f5-cd44-44d6-a572-c827f56ea132', 76, '2017-11-08 00:00:00', 'fb4d44b2_4d81_4e65_a5f7_21a52319c197');");
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('e2aa766d-f351-4504-b71a-512538bed910', 46, '2017-11-08 00:00:00', '4777eb44_178b_406c_9b49_9adf3f20f6e9');");
            pt1.operations.put(3, cohort3ops);
        }

        try {
            nm.TransactionQueue.put(pt1);
            Thread.sleep(2000);
            nm.CrashEmulationConfigs.get(1).reset();
            nm.TransactionTimestamps.get(1).clear();
            nm.MessageQueues.get(1).clear();
            ct1 = new CohortTask(nm, 1, nm.MessageQueues.get(1));
            ctct1 = new CohortTimeoutCheckingTask(nm, 1);
            executor.execute(ct1);
            executor.execute(ctct1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }

    @Test
    public void testAtomicityCoordinatorDownAfterCommit() throws InterruptedException{
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        nm.CrashEmulationConfigs.get(0).COORDINATOR_CRASH_AFTER_DECISION = true;

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


        PreparedTransaction pt1 = new PreparedTransaction("T-1");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            //cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
            pt1.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('b84af49f-5740-4e18-9912-33a077714c3d', 98, '2017-11-08 00:00:00', '79be1f14_c765_4cba_9420_35c0b78185b9');");
            pt1.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('05a164f5-cd44-44d6-a572-c827f56ea132', 76, '2017-11-08 00:00:00', 'fb4d44b2_4d81_4e65_a5f7_21a52319c197');");
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('e2aa766d-f351-4504-b71a-512538bed910', 46, '2017-11-08 00:00:00', '4777eb44_178b_406c_9b49_9adf3f20f6e9');");
            pt1.operations.put(3, cohort3ops);
        }

        try {
            nm.TransactionQueue.put(pt1);
            Thread.sleep(5000);
            nm.voteStatus.clear();
            nm.ackCount.clear();
            nm.preparing.clear();
            nm.CrashEmulationConfigs.get(0).reset();
            nm.TransactionTimestamps.get(0).clear();
            nm.MessageQueues.get(0).clear();
            cmht = new CoordinatorMessageHandlerTask(nm);
            ctct = new CoordinatorTimeoutCheckingTask(nm);
            executor.execute(cmht);
            executor.execute(ctct);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }

    @Test
    public void testAtomicityCoordinatorDownAfterAbort() throws InterruptedException{
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        nm.CrashEmulationConfigs.get(0).COORDINATOR_CRASH_AFTER_DECISION = true;

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


        PreparedTransaction pt1 = new PreparedTransaction("T-1");
        {
            List<String> cohort1ops = new ArrayList<>();
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('0e45d9c3-2cc9-4f7d-86e8-758325523cf7', 88, '2017-11-08 00:00:00', 'b37dfb72_d380_43c3_a681_60a87ecc797c');");
            cohort1ops.add("INSERT INTO thermometerobservation VALUES ('e36fe67f-04ae-4cd5-9860-9d8ca0b6c7e3', 38, '2017-11-08 00:00:00', '6978a208_ba81_4ffa_acc1_64fb3feda51d');");
            pt1.operations.put(1, cohort1ops);

            List<String> cohort2ops = new ArrayList<>();
            cohort2ops.add("INSERT INTO thermometerobservation VALUES ('b84af49f-5740-4e18-9912-33a077714c3d', 98, '2017-11-08 00:00:00', '79be1f14_c765_4cba_9420_35c0b78185b9');");
            pt1.operations.put(2, cohort2ops);

            List<String> cohort3ops = new ArrayList<>();
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('05a164f5-cd44-44d6-a572-c827f56ea132', 76, '2017-11-08 00:00:00', 'fb4d44b2_4d81_4e65_a5f7_21a52319c197');");
            cohort3ops.add("INSERT INTO thermometerobservation VALUES ('e2aa766d-f351-4504-b71a-512538bed910', 46, '2017-11-08 00:00:00', '4777eb44_178b_406c_9b49_9adf3f20f6e9');");
            pt1.operations.put(3, cohort3ops);
        }

        try {
            nm.TransactionQueue.put(pt1);
            Thread.sleep(2000);
            nm.voteStatus.clear();
            nm.ackCount.clear();
            nm.preparing.clear();
            nm.CrashEmulationConfigs.get(0).reset();
            nm.TransactionTimestamps.get(0).clear();
            nm.MessageQueues.get(0).clear();
            cmht = new CoordinatorMessageHandlerTask(nm);
            ctct = new CoordinatorTimeoutCheckingTask(nm);
            executor.execute(cmht);
            executor.execute(ctct);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        executor.shutdown();
        executor.awaitTermination(30L, TimeUnit.SECONDS);
    }
}
