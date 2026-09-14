package org.apache.cassandra.analytics;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.vdurmont.semver4j.Semver;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class BulkReaderSaiTest extends SharedClusterSparkIntegrationTestBase
{
    private static final QualifiedName TABLE =
    uniqueTestTableFullName(TEST_KEYSPACE);

    private static final int MATCHING_SCORE = 777;

    private static final int KEEP_ID = 1001;
    private static final int DELETE_ID = 1002;
    private static final int CHANGE_ID = 1003;
    private static final int NEW_MATCH_ID = 1004;

    // Make a full SSTable scan meaningfully larger than a point lookup.
    private static final int BACKGROUND_ROWS = 256;
    private static final String PAYLOAD = payload(16 * 1024);

    private static final CountingStats TEST_STATS = new CountingStats();

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(TestUtils.getDTestClusterVersion()
                            .isGreaterThanOrEqualTo(
                            new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("SAI requires Cassandra 5.0+")
        .isTrue();
    }

    @Test
    void testSparkSqlWhereUsesSaiAndPreservesReconciliation() throws Exception
    {
        ByteBuddyAgent.install();

        ClassReloadingStrategy strategy =
        ClassReloadingStrategy.fromInstalledAgent();

        try
        {
            // CassandraDataLayer normally returns DoNothingStats.
            // Replace it so we can prove the SAI query physically reads
            // less Data.db data than a full scan.
            new ByteBuddy()
            .redefine(CassandraDataLayer.class)
            .method(ElementMatchers.named("stats"))
            .intercept(MethodCall.invoke(
            BulkReaderSaiTest.class.getMethod("testStats")))
            .make()
            .load(CassandraDataLayer.class.getClassLoader(), strategy);

            // Baseline: full SSTable scan.
            TEST_STATS.reset();

            List<Row> allRows =
            bulkReaderDataFrame(TABLE)
            .load()
            .select("id", "score", "state", "payload")
            .collectAsList();

            long fullScanBytes = TEST_STATS.readBytes();

            assertThat(allRows)
            .hasSize(BACKGROUND_ROWS + 3);

            assertThat(fullScanBytes)
            .as("full scan should read a meaningful amount of Data.db")
            .isGreaterThan(1024 * 1024);

            // Actual SAI query.
            TEST_STATS.reset();

            Dataset<Row> input = bulkReaderDataFrame(TABLE).load();
            input.createOrReplaceTempView("sai_reader_it");

            List<Row> matches =
            getOrCreateSparkSession()
            .sql("SELECT id, score, state, payload "
                 + "FROM sai_reader_it "
                 + "WHERE score = " + MATCHING_SCORE)
            .collectAsList();

            long saiReadBytes = TEST_STATS.readBytes();

            List<Integer> ids =
            matches.stream()
                   .map(row -> row.getInt(0))
                   .sorted()
                   .collect(Collectors.toList());

            assertThat(ids)
            .containsExactly(KEEP_ID, NEW_MATCH_ID);

            Row keep = matches.stream()
                              .filter(row -> row.getInt(0) == KEEP_ID)
                              .findFirst()
                              .orElseThrow();

            assertThat(keep.getInt(1)).isEqualTo(MATCHING_SCORE);
            assertThat(keep.getString(2)).isEqualTo("updated");

            Row newMatch = matches.stream()
                                  .filter(row -> row.getInt(0) == NEW_MATCH_ID)
                                  .findFirst()
                                  .orElseThrow();

            assertThat(newMatch.getString(2)).isEqualTo("new-match");

            // Important: prove we did not simply fall back to a full scan.
            assertThat(saiReadBytes)
            .as("SAI predicate should substantially reduce Data.db reads; "
                + "fullScanBytes=%s saiReadBytes=%s",
                fullScanBytes, saiReadBytes)
            .isGreaterThan(0)
            .isLessThan(fullScanBytes / 3);
        }
        finally
        {
            strategy.reset(CassandraDataLayer.class);
        }
    }

    public static Stats testStats()
    {
        return TEST_STATS;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        createTestTable(
        TABLE,
        "CREATE TABLE IF NOT EXISTS %s ("
        + "id int PRIMARY KEY, "
        + "score int, "
        + "state text, "
        + "payload text"
        + ") WITH compression = {'enabled': 'false'} "
        + "AND compaction = {"
        + "'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', "
        + "'enabled': 'false'"
        + "};");

        ICoordinator coordinator = cluster.get(1).coordinator();

        coordinator.execute(
        String.format(
        "CREATE INDEX IF NOT EXISTS score_sai "
        + "ON %s (score) USING 'sai';",
        TABLE),
        ConsistencyLevel.ALL);

        // Lots of data which does not match score=777. The relatively
        // large payload gives us a robust full-scan I/O baseline.
        for (int i = 0; i < BACKGROUND_ROWS; i++)
        {
            insert(coordinator, i, i, "background");
        }

        // All three are SAI matches in SSTable #1.
        insert(coordinator, KEEP_ID, MATCHING_SCORE, "original");
        insert(coordinator, DELETE_ID, MATCHING_SCORE, "delete-me");
        insert(coordinator, CHANGE_ID, MATCHING_SCORE, "change-score");

        flushTable();

        // SSTable #2:
        //
        // KEEP_ID:
        //   Candidate comes from the older SAI index, but the newer
        //   non-indexed column value must win during reconciliation.
        coordinator.execute(
        String.format(
        "UPDATE %s SET state = 'updated' WHERE id = %d",
        TABLE, KEEP_ID),
        ConsistencyLevel.ALL);

        // DELETE_ID:
        // Older SAI has a hit, newer SSTable contains a tombstone.
        coordinator.execute(
        String.format(
        "DELETE FROM %s WHERE id = %d",
        TABLE, DELETE_ID),
        ConsistencyLevel.ALL);

        // CHANGE_ID:
        // Older SAI has score=777. New value is 778.
        // Spark's residual predicate must filter this partition.
        coordinator.execute(
        String.format(
        "UPDATE %s SET score = 778, state = 'changed' "
        + "WHERE id = %d",
        TABLE, CHANGE_ID),
        ConsistencyLevel.ALL);

        // Match existing only in the second SSTable.
        insert(coordinator, NEW_MATCH_ID, MATCHING_SCORE, "new-match");

        flushTable();
    }

    private static void insert(ICoordinator coordinator,
                               int id,
                               int score,
                               String state)
    {
        coordinator.execute(
        String.format(
        "INSERT INTO %s "
        + "(id, score, state, payload) "
        + "VALUES (%d, %d, '%s', '%s')",
        TABLE, id, score, state, PAYLOAD),
        ConsistencyLevel.ALL);
    }

    private void flushTable()
    {
        cluster.stream()
               .forEach(instance ->
                        instance.nodetool(
                        "flush",
                        TEST_KEYSPACE,
                        TABLE.table()));
    }

    private static String payload(int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }

    private static final class CountingStats extends Stats
    {
        private final AtomicLong bytes = new AtomicLong();

        @Override
        public void readBytes(int length)
        {
            bytes.addAndGet(length);
        }

        long readBytes()
        {
            return bytes.get();
        }

        void reset()
        {
            bytes.set(0);
        }
    }
}