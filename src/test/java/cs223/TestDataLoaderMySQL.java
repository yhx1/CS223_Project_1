package cs223;

import com.mysql.cj.exceptions.CJCommunicationsException;
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
    public void testMySQLBenchmark() throws CJCommunicationsException {
        try{
            MySQLBenchmark mb = new MySQLBenchmark();

            Metric metric = mb.runMySQLBenchmark();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
