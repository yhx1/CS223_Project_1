package cs223;

import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class TestDataLoaderPostgres {

    public void testCreateSchema() {
        try{
            PostgresDataLoader.RunSQLByFile("Resources/schema/create.sql");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void testDropSchema() {
        try{
            PostgresDataLoader.RunSQLByLine("Resources/schema/drop.sql");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void testLoadMetadata() {
        try{
            PostgresDataLoader.RunSQLByLine("Resources/data/high_concurrency/metadata.sql");
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
    public void testParseTimestamp() {
        try{
            PostgresDataLoader.ParseTimestamp("2017-11-08 00:00:00");
            PostgresDataLoader.ParseTimestamp("2017-11-27 23:57:59");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
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
    public void testPostgresBenchmark() {
        try{
            PostgresBenchmark pb = new PostgresBenchmark(
                    "Resources/data/low_concurrency/observation_low_concurrency.sql",
                    "Resources/data/low_concurrency/semantic_observation_low_concurrency.sql"
            );

            Metric metric = pb.runPostgresBenchmark();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
