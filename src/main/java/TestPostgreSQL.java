
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestPostgreSQL {


    public static void main(final String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:5432/testdb";
        String user = "postgres";
        String password = "cs223p1";

        Connection con = DriverManager.getConnection(url, user, password);
        con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        int TransactionIsolationLevel = con.getTransactionIsolation();
        System.out.println("Transaction Isolation level: "+ TransactionIsolationLevel);
        try {
            //con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            String query1 = ("SELECT VERSION()");
            String query2 = ("SELECT * from garbage");

            List<String> queries = new ArrayList<>();
            queries.add(query1);
            queries.add(query2);

            for (int i=0; i < queries.size(); i++) {
                Statement st = con.createStatement();
                ResultSet rs = st.executeQuery(queries.get(i));

                if (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }

            con.commit();
        }
        catch (SQLException e)
        {
            System.out.println(e);
            //Logger lgr = Logger.getLogger(JavaPostgreSqlVersion.class.getName());
            //lgr.log(Level.SEVERE, ex.getMessage(), ex);
            con.rollback();
        }

    }

}
