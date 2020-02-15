package cs223;
import java.io.*;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class PostgresDataLoader {

    static String SCHEMA_URL_PREFIX = "Resources/schema/";
    static String DATA_URL_PREFIX = "Resources/data/";
    public static String DB_URL = "jdbc:postgresql://localhost:5432/testdb";
    public static String DB_USER = "postgres";
    public static String DB_PASSWORD = "cs223p1postgres";


    public static void RunSQLByFile(String filename) throws Exception {
        String filepath = filename;
        String content = new String(Files.readAllBytes(Paths.get(filepath)));
        //System.out.println(content);

        Connection con = DriverManager.getConnection(DB_URL, DB_USER,DB_PASSWORD);
        Statement st = con.createStatement();
        st.executeUpdate(content);
    }

    public static void RunSQLByLine(String filename) throws Exception {
        Connection con = DriverManager.getConnection(DB_URL, DB_USER,DB_PASSWORD);

        BufferedReader reader;

        String filepath = filename;

        reader = new BufferedReader(new FileReader(filepath));

        String line = reader.readLine();
        while(line != null) {
            Statement st = con.createStatement();
            st.executeUpdate(line);
            line = reader.readLine();
        }

    }

    public static int ParseTimestamp(String timestamp) {
        int day = Integer.parseInt(timestamp.substring(8,10)) - 8;
        int hour = Integer.parseInt(timestamp.substring(11,13));
        int minute = Integer.parseInt(timestamp.substring(14,16));
        int second = Integer.parseInt(timestamp.substring(17,19));

        int converted = second + (60 * minute) + (3600 * hour) + (86400 * day);
        converted = converted / Settings.TIME_UNIT_SECS;

        //System.out.println("Converted Unit: " + converted + " Day: " + day + " Hour: " + hour + " Minute: " + minute + " Second: " + second);
        return converted;
    }

    public static boolean MatchTableName(String TableName, String line) {
        if (line.length() < 6) {
            return false;
        }
        if (!(line.substring(0,6).equalsIgnoreCase("INSERT"))){
            //System.out.println("Not an insert: "+line);
            return false;
        }
        Pattern p1 = Pattern.compile("\\s+");
        String[] tempString = p1.split(line);
        String currentTableName = tempString[2];
        if (TableName.equals(currentTableName)) {
            return true;
        }
        return false;
    }

    /*
    public static String[] PreprocessLine(String TableName, String line) {
        if (line.length() < 6) {
            return new String[]{};
        }
        if (!(line.substring(0,6).equalsIgnoreCase("INSERT"))){
            //System.out.println("Not an insert: "+line);
            return new String[]{};
        }
        Pattern p2 = Pattern.compile("\\(");
        String[] tempString = p2.split(line);
        Pattern p3 = Pattern.compile(",");
        String[] tempString2 = p3.split(tempString[1].substring(0, tempString[1].length()-2));
        for (int i=0; i<tempString2.length; i++) {
            tempString2[i] = tempString2[i].trim().substring(1,tempString2[i].length()-1);
        }
        String TimeStamp, Sensor;
        switch(TableName) {
            case "thermometerobservation": {
                TimeStamp = tempString2[2];
                Sensor = tempString2[3];
                //System.out.println("THERMO TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                break;
            }
            case "wemoobservation": {
                TimeStamp = tempString2[3];
                Sensor = tempString2[4];
                //System.out.println("WEMO TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                break;
            }
            case "wifiapobservation":
                TimeStamp = tempString2[2];
                Sensor = tempString2[3];
                //System.out.println("WIFIAP TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                break;
            case "occupancy": {
                TimeStamp = tempString2[3];
                Sensor = tempString2[4];
                //System.out.println("OCCUPANCY TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                break;
            }
            case "presence": {
                TimeStamp = tempString2[3];
                Sensor = tempString2[4];
                //System.out.println("PRESENCE TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                break;
            }
            default: {
                TimeStamp = "";
                Sensor = "";
                break;
            }
        }
        return new String[]{TimeStamp, Sensor};
    }
    */

    public static void PreprocessInserts(String filename) throws Exception {

        String preprocessLoc = Settings.PREPROCESSED_DATA_URL;
        boolean garbage = new File(preprocessLoc).mkdir();

        TreeMap<Integer, HashMap<String, ArrayList<String>>> cachedStatements = new TreeMap<Integer, HashMap<String, ArrayList<String>>>();

        BufferedReader reader;
        reader = new BufferedReader(new FileReader(filename));

        Pattern p1 = Pattern.compile("\\s+");
        Pattern p2 = Pattern.compile("\\(");
        Pattern p3 = Pattern.compile(",");

        int count = 0;

        String line = reader.readLine();
        while (line != null) {
            count++;
            if (line.length() < 6) {
                line = reader.readLine();
                continue;
            }
            if (!(line.substring(0,6).equalsIgnoreCase("INSERT"))){
                System.out.println("Not an insert: "+line);
                line = reader.readLine();
                continue;
            }

            String[] tempString = p1.split(line);
            String TableName = tempString[2];


            //System.out.println(TableName);
            tempString = p2.split(line);
            //System.out.println(tempString[1]);

            String[] tempString2 = p3.split(tempString[1].substring(0, tempString[1].length()-2));

            for (int i=0; i<tempString2.length; i++) {
                //tempString2[i] = tempString2[i].trim().replaceAll("'","");
                tempString2[i] = tempString2[i].trim().substring(1,tempString2[i].length()-1);
                //System.out.print(tempString2[i]+ " * ");
            }
            //System.out.println(" ");

            String TimeStamp, Sensor;
            switch(TableName) {
                case "thermometerobservation": {
                    TimeStamp = tempString2[2];
                    Sensor = tempString2[3];
                    //System.out.println("THERMO TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                    break;
                }
                case "wemoobservation": {
                    TimeStamp = tempString2[3];
                    Sensor = tempString2[4];
                    //System.out.println("WEMO TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                    break;
                }
                case "wifiapobservation":
                    TimeStamp = tempString2[2];
                    Sensor = tempString2[3];
                    //System.out.println("WIFIAP TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                    break;
                case "occupancy": {
                    TimeStamp = tempString2[3];
                    Sensor = tempString2[4];
                    //System.out.println("OCCUPANCY TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                    break;
                }
                case "presence": {
                    TimeStamp = tempString2[3];
                    Sensor = tempString2[4];
                    //System.out.println("PRESENCE TimeStamp: " + TimeStamp + " Sensor: " + Sensor);
                    break;
                }
                default: {
                    TimeStamp = "";
                    Sensor = "";
                    break;
                }
            }


            int TimeUnit = ParseTimestamp(TimeStamp);
            String SensorID = TableName + "_" + Sensor;

            // Cache statements
            {
                if (!cachedStatements.containsKey(TimeUnit)) {
                    cachedStatements.put(TimeUnit, new HashMap<String, ArrayList<String>>());
                }

                if (!cachedStatements.get(TimeUnit).containsKey(SensorID)) {
                    cachedStatements.get(TimeUnit).put(SensorID, new ArrayList<String>());
                }

                cachedStatements.get(TimeUnit).get(SensorID).add(line);
            }


            // Write cache to disk
            if (count >= 100000)
            {
                count = 0;

                Iterator<Integer> insertsIterator = cachedStatements.keySet().iterator();

                while (insertsIterator.hasNext()) {

                    int time = insertsIterator.next();
                    HashMap<String, ArrayList<String>> currentStatements = cachedStatements.get(time);

                    String storageFilenamePrefix = Settings.PREPROCESSED_DATA_URL + time + "_";
                    int segmentNumber = 0;
                    File storageFile = new File(storageFilenamePrefix + segmentNumber);
                    while (storageFile.exists()) {
                        segmentNumber++;
                        storageFile = new File(storageFilenamePrefix + segmentNumber);
                    }
                    if (!storageFile.createNewFile()){
                        throw new IOException("Cannot create file");
                    }

                    String storageFilename = storageFilenamePrefix + segmentNumber;
                    FileOutputStream fos = new FileOutputStream(storageFilename);
                    ObjectOutputStream oos = new ObjectOutputStream(fos);
                    oos.writeObject(currentStatements);
                    oos.close();
                    fos.close();

                }

                cachedStatements.clear();
            }


            line = reader.readLine();

        }

        Iterator<Integer> insertsIterator = cachedStatements.keySet().iterator();

        while (insertsIterator.hasNext()) {

            int time = insertsIterator.next();
            HashMap<String, ArrayList<String>> currentStatements = cachedStatements.get(time);

            String storageFilenamePrefix = Settings.PREPROCESSED_DATA_URL + time + "_";
            int segmentNumber = 0;
            File storageFile = new File(storageFilenamePrefix + segmentNumber);
            while (storageFile.exists()) {
                segmentNumber++;
                storageFile = new File(storageFilenamePrefix + segmentNumber);
            }
            if (!storageFile.createNewFile()){
                throw new IOException("Cannot create file");
            }

            String storageFilename = storageFilenamePrefix + segmentNumber;
            FileOutputStream fos = new FileOutputStream(storageFilename);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(currentStatements);
            oos.close();
            fos.close();
        }

        cachedStatements.clear();

    }



}
