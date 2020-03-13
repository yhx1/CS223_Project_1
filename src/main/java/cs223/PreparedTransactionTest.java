package cs223;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static cs223.PostgresDataLoader.*;

public class PreparedTransactionTest {

    public static void main(final String[] args) {

        ArrayList<String> statements = new ArrayList<>();
        String COMMIT_STRING = "ROLLBACK PREPARED '40';";


        statements.add("BEGIN;");
        statements.add("INSERT INTO thermometerobservation VALUES ('215b6bc5-ee58-40aa-a4ae-b594220540c9', 24, '2017-11-08 00:00:00', '7ec9f039_d2e9_4e77_b837_677f61702693');");
        statements.add("PREPARE TRANSACTION '40';");

        Connection con = null;
        Statement st = null;
        try {
            con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(false);

            for (int i=0; i < statements.size(); i++) {
                st = con.createStatement();
                boolean rs = st.execute(statements.get(i));
                //if (!rs) {
                //    System.out.println("Error occurred: "+statements.get(i));
                //}
                st.close();
            }

            con.commit();
        }
        catch (Exception e) {
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

        try {
            con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);

            st = con.createStatement();
            boolean rs = st.execute(COMMIT_STRING);

            st.close();
        }
        catch (Exception e) {
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

}
