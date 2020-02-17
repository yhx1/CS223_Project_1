package cs223;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Benchmark {

    public static List<Integer> MPLs = new ArrayList<Integer>(Arrays.asList(2,5,10,20,50));
    public static List<Integer> Levels = new ArrayList<>(Arrays.asList(Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE));

    public static void postgresCleanUp() {
        try{
            PostgresDataLoader.RunSQLByLine("Resources/schema/drop.sql");
            PostgresDataLoader.RunSQLByFile("Resources/schema/create.sql");
            PostgresDataLoader.RunSQLByLine(Settings.METADATA_DATASET_URL);
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

    public static void mysqlCleanUp() {
        try{
            MySQLDataLoader.RunSQLByLine("mysql/schema/drop_mysql.sql", false);
            MySQLDataLoader.RunSQLByLine("mysql/schema/create_mysql.sql", true);
            MySQLDataLoader.RunSQLByLine(Settings.METADATA_DATASET_URL, true );
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

    public static void runBenchmarks() throws Exception{

        for (int i = 0; i < MPLs.size(); i++) {
            Settings.MPL = MPLs.get(i);

            for (int j = 0; j < Levels.size(); j++) {
                Settings.ISOLATION_LEVEL = Levels.get(j);

                System.out.println("MPL: "+ Settings.MPL + " Level: "+ Settings.ISOLATION_LEVEL);

                postgresCleanUp();
                PostgresBenchmark pb = new PostgresBenchmark();
                pb.runPostgresBenchmark();
            }

        }

        for (int i = 0; i < MPLs.size(); i++) {
            Settings.MPL = MPLs.get(i);

            for (int j = 0; j < Levels.size(); j++) {
                Settings.ISOLATION_LEVEL = Levels.get(j);

                System.out.println("MPL: "+ Settings.MPL + " Level: "+ Settings.ISOLATION_LEVEL);

                mysqlCleanUp();
                MySQLBenchmark mb = new MySQLBenchmark();
                mb.runMySQLBenchmark();
            }

        }



    }

    public static void main(final String[] args) throws Exception{
        if (Settings.HIGH_CONCURRENCY) {
            Settings.switch_to_high_concurrency();
        }
        PostgresDataLoader.PreprocessInserts(Settings.SEMANTIC_DATASET_URL);
        PostgresDataLoader.PreprocessInserts(Settings.OBSERVATION_DATASET_URL);
        runBenchmarks();
    }

}
