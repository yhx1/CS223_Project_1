package cs223;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MySQLBenchmark {

    public final static String[] ObservationTableNames = {"thermometerobservation","wemoobservation","wifiapobservation"};
    public final static String[] SemanticTableNames = {"occupancy", "presence"};

    public static TreeMap<Integer, HashMap<String, ArrayList<String>>> queryStatements = new TreeMap<Integer, HashMap<String, ArrayList<String>>>();

    public MySQLBenchmark() {
    }

    public void convertQueriesToMySQL() {

        for (int time = 0; time < 0.5 * 1728000 / Settings.TIME_UNIT_SECS; time++) {

            ArrayList<String> currentQueries = queryStatements.get(time).get("Query");

            for (int i = 0 ; i < currentQueries.size(); i++) {

                {
                    String replaced = "date_trunc('day', ";
                    String alternativePart1 = "DATE_FORMAT(";
                    String alternativePart2 = ", '%Y-%m-%d 00:00:00')";
                    String query = currentQueries.get(i);

                    int idx;
                    while ((idx = query.indexOf(replaced)) != -1) {
                        StringBuffer sb = new StringBuffer(query);
                        sb.replace(idx, idx + replaced.length(), alternativePart1);
                        int j = idx;
                        while (sb.charAt(j) != ')') {
                            j++;
                        }
                        if (j >= sb.length()) {
                            break;
                        }
                        sb.replace(j, j + 1, alternativePart2);

                        query = sb.toString();
                    }

                    currentQueries.set(i, query);
                }

                {
                    String replaced = "=ANY(array[";
                    String alternativePart1 = " IN (";

                    String query = currentQueries.get(i);

                    int idx;
                    while ((idx = query.indexOf(replaced)) != -1) {
                        StringBuffer sb = new StringBuffer(query);
                        sb.replace(idx, idx + replaced.length(), alternativePart1);
                        int j = idx;
                        while (sb.charAt(j) != ']') {
                            j++;
                        }
                        if (j >= sb.length()) {
                            break;
                        }
                        sb.replace(j, j + 1, "");

                        query = sb.toString();
                    }

                    currentQueries.set(i, query);
                }

            }

        }

        System.out.println("Converted to MySQL compatible queries.");

    }


    public void runMySQLBenchmarkOnTick(ThreadPoolExecutor executor, int time, Metric metric) throws Exception {
        String storageFilenamePrefix = Settings.PREPROCESSED_DATA_URL + time + "_";
        int segmentNumber = 0;
        File storageFile = new File(storageFilenamePrefix + segmentNumber);
        HashMap<String, ArrayList<String>> currentStatements;
        if (queryStatements.containsKey(time)) {
            currentStatements = queryStatements.get(time);
        } else {
            currentStatements = new HashMap<String, ArrayList<String>>();
        }

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
            if (sensorID.equals("Query")) {
                for (int j = 0; j < currentStatements.get(sensorID).size(); j++) {
                    taskStartTime = System.currentTimeMillis();
                    ArrayList temp = new ArrayList<String>();
                    temp.add(currentStatements.get(sensorID).get(j));
                    MySQLTransactionTask task = new MySQLTransactionTask(temp, metric, true, taskStartTime);
                    executor.execute(task);
                }
            }
            else if (Settings.DO_NOT_GROUP_DATA_OPERATIONS) {
                for (int j = 0; j < currentStatements.get(sensorID).size(); j++) {
                    taskStartTime = System.currentTimeMillis();
                    ArrayList temp = new ArrayList<String>();
                    temp.add(currentStatements.get(sensorID).get(j));
                    MySQLTransactionTask task = new MySQLTransactionTask(temp, metric, false, taskStartTime);
                    executor.execute(task);
                }
            }
            else {
                taskStartTime = System.currentTimeMillis();
                MySQLTransactionTask task = new MySQLTransactionTask(currentStatements.get(sensorID), metric, false, taskStartTime);
                executor.execute(task);
            }
        }
        //metric.printMetrics(time);
    }

    public Metric runMySQLBenchmark() throws Exception {

        queryStatements = QueryParser.parseTime(Settings.QUERY_DATA_URL);
        convertQueriesToMySQL();

        Metric metric = new Metric();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MPL);

        for (int time = 0; time < Settings.TEST_RUNNING_TIME_SECS; time++) {
            runMySQLBenchmarkOnTick(executor, time, metric);
            Thread.sleep(Settings.INTERVAL_BETWEEN_TIME_UNIT);
        }

        metric.printMetrics(Settings.TEST_RUNNING_TIME_SECS);

        executor.shutdownNow();
        Thread.sleep(1000);
        return metric;
    }



}
