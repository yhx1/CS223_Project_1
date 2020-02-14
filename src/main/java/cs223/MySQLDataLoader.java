package cs223;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MySQLDataLoader {

    public static String DB_URL = "jdbc:mysql://localhost:3306/testdb";
    public static String DB_USER = "cs223p1";
    public static String DB_PASSWORD = "cs223p1";

    public static void RunSQLByFile(String filename) throws Exception {
        String filepath = filename;
        String content = new String(Files.readAllBytes(Paths.get(filepath)));
        //System.out.println(content);

        Connection con = DriverManager.getConnection(DB_URL, DB_USER,DB_PASSWORD);
        Statement st = con.createStatement();
        st.executeUpdate(content);
    }

    public static void RunSQLByLine(String filename, boolean ignoreSET) throws Exception {
        Connection con = DriverManager.getConnection(DB_URL, DB_USER,DB_PASSWORD);

        BufferedReader reader;

        String filepath = filename;

        reader = new BufferedReader(new FileReader(filepath));

        String line = reader.readLine();
        while(line != null) {
            //System.out.println(line);

            if (line.length() < 10) {
                line = reader.readLine();
                continue;
            }
            /*
            if (line.contains("/*")) {
                line = reader.readLine();
                continue;
            }

             */
            if (ignoreSET && line.substring(0,3).equalsIgnoreCase("SET")) {
                line = reader.readLine();
                continue;
            }
            Statement st = con.createStatement();
            st.executeUpdate(line);
            line = reader.readLine();
        }

    }

}
