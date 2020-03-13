package cs223;

import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import static cs223.PostgresDataLoader.*;

public class TestDataLoaderPostgres {

    public void testCreateSchema() {
        try{
            PostgresDataLoader.RunSQLByFile("Resources/schema/create.sql", DB_URL, DB_USER, DB_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void testDropSchema() {
        try{
            PostgresDataLoader.RunSQLByLine("Resources/schema/drop.sql", DB_URL, DB_USER, DB_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    public void testLoadMetadata() {
        try{
            PostgresDataLoader.RunSQLByLine("Resources/data/low_concurrency/metadata.sql", DB_URL, DB_USER, DB_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void cleanUp() {
        testDropSchema();
        testCreateSchema();
        testLoadMetadata();
        System.out.println("Recreated Schemas and loaded Metadata.");
    }

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
            PostgresDataLoader.PreprocessInserts("Resources/data/low_concurrency/semantic_observation_low_concurrency.sql");
            PostgresDataLoader.PreprocessInserts("Resources/data/low_concurrency/observation_low_concurrency.sql");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testPostgresBenchmark() {
        try{
            PostgresBenchmark pb = new PostgresBenchmark();

            Metric metric = pb.runPostgresBenchmark();

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
