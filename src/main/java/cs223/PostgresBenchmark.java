package cs223;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class PostgresBenchmark {

    public final static String[] ObservationTableNames = {"thermometerobservation","wemoobservation","wifiapobservation"};
    public final static String[] SemanticTableNames = {"occupancy", "presence"};

    public static TreeMap<Integer, HashMap<String, ArrayList<String>>> queryStatements = new TreeMap<Integer, HashMap<String, ArrayList<String>>>();

    public String observationURL, semanticURL;

    public PostgresBenchmark(String observationURL, String semanticURL) {
        this.observationURL = observationURL;
        this.semanticURL = semanticURL;
    }

    public void runPostgresBenchmarkOnTick(ThreadPoolExecutor executor, int time, Metric metric) throws Exception {
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
                    PostgresTransactionTask task = new PostgresTransactionTask(temp, metric, true, taskStartTime);
                    executor.execute(task);
                }
            } else {
                taskStartTime = System.currentTimeMillis();
                PostgresTransactionTask task = new PostgresTransactionTask(currentStatements.get(sensorID), metric, false, taskStartTime);
                executor.execute(task);
            }
        }
        metric.printMetrics(time);
    }

    public Metric runPostgresBenchmark() throws Exception {

        Metric metric = new Metric();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MPL);

        for (int time = 0; time < 0.5 * 1728000 / Settings.TIME_UNIT_SECS; time++) {
            runPostgresBenchmarkOnTick(executor, time, metric);
            Thread.sleep(Settings.INTERVAL_BETWEEN_TIME_UNIT);
        }

        return metric;
    }

}
