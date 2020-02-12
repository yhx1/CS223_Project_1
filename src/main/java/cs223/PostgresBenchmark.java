package cs223;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class PostgresBenchmark {

    public final static String[] ObservationTableNames = {"thermometerobservation","wemoobservation","wifiapobservation"};
    public final static String[] SemanticTableNames = {"occupancy", "presence"};


    //public static TreeMap<Integer, HashMap<String, ArrayList<String>>> statements;

    public String observationURL, semanticURL;

    public PostgresBenchmark(String observationURL, String semanticURL) {
        this.observationURL = observationURL;
        this.semanticURL = semanticURL;
    }

    /*
    public Metric runPostgresBenchmarkInPlace() throws InterruptedException, IOException {
        Metric metric = new Metric();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MPL);

        List<String> TableNames = new ArrayList<>();
        List<BufferedReader> readers = new ArrayList<>();
        List<String> currentLines = new ArrayList<>();

        for (int i = 0; i < ObservationTableNames.length; i++) {
            BufferedReader reader = new BufferedReader(new FileReader(observationURL));
            String line = reader.readLine();
            while (!PostgresDataLoader.MatchTableName(ObservationTableNames[i], line)) {
                line = reader.readLine();
            }
            System.out.println("Table Found: " + ObservationTableNames[i] + ", Line: " + line);
            currentLines.add(line);
            readers.add(reader);
            TableNames.add(ObservationTableNames[i]);
        }
        for (int i = 0; i < SemanticTableNames.length; i++) {
            BufferedReader reader = new BufferedReader(new FileReader(semanticURL));
            String line = reader.readLine();
            while (!PostgresDataLoader.MatchTableName(SemanticTableNames[i], line)) {
                line = reader.readLine();
            }
            System.out.println("Table Found: " + SemanticTableNames[i] + ", Line: " + line);
            currentLines.add(line);
            readers.add(reader);
            TableNames.add(SemanticTableNames[i]);
        }

        for (int time = 0; time < 1728000 / Settings.TIME_UNIT_SECS; time++) {

            HashMap<String, ArrayList<String>> currentStatements = new HashMap<>();

            for (int i = 0; i < readers.size(); i++) {
                String TableName = TableNames.get(i);
                String currentLine = currentLines.get(i);
                BufferedReader currentReader = readers.get(i);

                String[] timeAndSensor = PostgresDataLoader.PreprocessLine(TableName, currentLine);
                String sensorID = TableName + "_" + timeAndSensor[1];

                while (!timeAndSensor[0].equals("") &&  PostgresDataLoader.ParseTimestamp(timeAndSensor[0]) == time) {

                    if (!currentStatements.containsKey(sensorID)){
                        currentStatements.put(sensorID, new ArrayList<>());
                    }

                    currentStatements.get(sensorID).add(currentLine);

                    currentLine = currentReader.readLine();
                    currentLines.set(i, currentLine);
                    timeAndSensor = PostgresDataLoader.PreprocessLine(TableName, currentLine);
                    //System.out.println("Line Read: "+currentLine);
                }

                System.out.println("Terminated, current line: " + currentLine);

            }

            Iterator<String> currentInsertsIterator = currentStatements.keySet().iterator();

            while (currentInsertsIterator.hasNext()) {
                String sensorID = currentInsertsIterator.next();
                //System.out.println(sensorID + ": " + currentStatements.get(sensorID));
                PostgresTransactionTask task = new PostgresTransactionTask(currentStatements.get(sensorID), metric, false);
                executor.execute(task);
            }
            //System.out.println("");
            metric.printMetrics();
            Thread.sleep(Settings.INTERVAL_BETWEEN_TIME_UNIT);

        }

        return metric;
    }
    */

    public void runPostgresBenchmarkOnTick(ThreadPoolExecutor executor, int time, Metric metric) throws Exception {
        String storageFilenamePrefix = Settings.PREPROCESSED_DATA_URL + time + "_";
        int segmentNumber = 0;
        File storageFile = new File(storageFilenamePrefix + segmentNumber);
        HashMap<String, ArrayList<String>> currentStatements = new HashMap<String, ArrayList<String>>();

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
            long taskStartTime = System.currentTimeMillis();
            PostgresTransactionTask task = new PostgresTransactionTask(currentStatements.get(sensorID), metric, false, taskStartTime);
            executor.execute(task);
        }
        metric.printMetrics(time);
    }

    public Metric runPostgresBenchmark() throws Exception {

        Metric metric = new Metric();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.MPL);

        //Iterator<Integer> insertsIterator = statements.keySet().iterator();

        for (int time = 0; time < 1728000 / Settings.TIME_UNIT_SECS; time++) {
            runPostgresBenchmarkOnTick(executor, time, metric);
            Thread.sleep(Settings.INTERVAL_BETWEEN_TIME_UNIT);
        }

        /*
        while (insertsIterator.hasNext()) {

            int time = insertsIterator.next();
            HashMap<String, ArrayList<String>> currentStatements = statements.get(time);

            Iterator<String> currentInsertsIterator = currentStatements.keySet().iterator();

            while (currentInsertsIterator.hasNext()) {
                String sensorID = currentInsertsIterator.next();
                PostgresTransactionTask task = new PostgresTransactionTask(currentStatements.get(sensorID), metric, false);
                executor.execute(task);
            }
            metric.printMetrics();
            Thread.sleep(Settings.INTERVAL_BETWEEN_TIME_UNIT);
        }

         */


        return metric;
    }



}
