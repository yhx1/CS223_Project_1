package cs223;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataLoaderMySQL {

    public void testCreateSchema() {
        try{
            MySQLDataLoader.RunSQLByLine("mysql/schema/create_mysql.sql", true);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void testDropSchema() {
        try{
            MySQLDataLoader.RunSQLByLine("mysql/schema/drop_mysql.sql", false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void testLoadMetadata() {
        try{
            MySQLDataLoader.RunSQLByLine("Resources/data/high_concurrency/metadata.sql", true );
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
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
    public void testPreprocessInserts() {
        try{
            PostgresDataLoader.PreprocessInserts("Resources/data/high_concurrency/semantic_observation_high_concurrency.sql");
            PostgresDataLoader.PreprocessInserts("Resources/data/high_concurrency/observation_high_concurrency.sql");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testMySQLBenchmark() {
        try{
            MySQLBenchmark mb = new MySQLBenchmark(
                    "Resources/data/low_concurrency/observation_low_concurrency.sql",
                    "Resources/data/low_concurrency/semantic_observation_low_concurrency.sql"
            );

            Metric metric = mb.runMySQLBenchmark();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
