/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.analytics;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
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
    private static final QualifiedName TABLE = uniqueTestTableFullName(TEST_KEYSPACE);

    // CassandraBridgeImplementation reconciles at most 1024 SAI candidate partition keys per Data.db scanner.
    // Use enough candidates to force a second scanner in the same Spark partition.
    private static final int SAI_RECONCILIATION_BATCH_SIZE = 1024;
    private static final int MATCHING_ROWS = 1100;
    private static final int TOTAL_ROWS = 2048;

    private static final int MATCHING_SCORE = 777;
    private static final int CHANGED_SCORE = 778;

    // Make the non-matching portion dominate the full-scan byte count without making the SAI result itself large.
    // This gives the readBytes assertion a comfortable margin while keeping the test data set reasonably small.
    private static final String MATCHING_PAYLOAD = payload(256);
    private static final String BACKGROUND_PAYLOAD = payload(4 * 1024);

    private static final CountingStats TEST_STATS = new CountingStats();

    private final Set<Integer> expectedMatchingIds = new HashSet<>();
    private int keepId;
    private int deleteId;
    private int changeId;
    private int newMatchId;

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(TestUtils.getDTestClusterVersion()
                            .isGreaterThanOrEqualTo(new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("SAI requires Cassandra 5.0+")
        .isTrue();
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        // One node with one token plus spark.default.parallelism=1 below gives this test exactly one Spark token range.
        // That makes MATCHING_ROWS > 1024 a deterministic exercise of BatchedCompactionStreamScanner's second batch.
        return super.testClusterConfiguration()
                    .nodesPerDc(1)
                    .tokenCount(1);
    }

    @Test
    void testSparkSqlWhereUsesSaiAcrossPagesAndReconciliationBatches() throws Exception
    {
        // SparkTestUtils normally uses local[8], which would split this one-token ring into 8 reader partitions.
        // Force a single read partition so >1024 SAI candidates necessarily cross the reconciliation batch boundary.
        getOrCreateSparkConf().set("spark.default.parallelism", "1");

        ByteBuddyAgent.install();
        ClassReloadingStrategy strategy = ClassReloadingStrategy.fromInstalledAgent();
        try
        {
            // CassandraDataLayer normally returns DoNothingStats. Replace it so a query that silently falls back to
            // a full Data.db scan cannot pass merely because Spark's residual predicate still returns correct rows.
            new ByteBuddy()
            .redefine(CassandraDataLayer.class)
            .method(ElementMatchers.named("stats"))
            .intercept(MethodCall.invoke(BulkReaderSaiTest.class.getMethod("testStats")))
            .make()
            .load(CassandraDataLayer.class.getClassLoader(), strategy);

            TEST_STATS.reset();
            List<Row> allRows = bulkReaderDataFrame(TABLE)
                                .load()
                                .select("id", "score", "state", "payload")
                                .collectAsList();
            long fullScanBytes = TEST_STATS.readBytes();

            assertThat(allRows).hasSize(TOTAL_ROWS - 1); // one partition is deleted in SSTable #2
            assertThat(fullScanBytes)
            .as("full scan should read a meaningful amount of Data.db")
            .isGreaterThan(1024 * 1024);

            TEST_STATS.reset();
            Dataset<Row> input = bulkReaderDataFrame(TABLE).load();
            assertThat(input.rdd().getNumPartitions())
            .as("the SAI query must use one Spark range so 1100 candidates cross the 1024-key batch boundary")
            .isEqualTo(1);
            input.createOrReplaceTempView("sai_reader_it");

            List<Row> matches = getOrCreateSparkSession()
                                .sql("SELECT id, score, state, payload "
                                     + "FROM sai_reader_it "
                                     + "WHERE score = " + MATCHING_SCORE)
                                .collectAsList();
            long saiReadBytes = TEST_STATS.readBytes();

            Set<Integer> actualMatchingIds = matches.stream()
                                                    .map(row -> row.getInt(0))
                                                    .collect(Collectors.toSet());

            // The result itself spans the batch boundary too; this catches losing/duplicating rows when CellIterator
            // switches from one CompactionStreamScanner (and RowData instance) to the next.
            assertThat(matches.size()).isGreaterThan(SAI_RECONCILIATION_BATCH_SIZE);
            assertThat(matches).hasSize(expectedMatchingIds.size());
            assertThat(actualMatchingIds)
            .hasSize(matches.size())
            .containsExactlyInAnyOrderElementsOf(expectedMatchingIds);
            assertThat(matches).allSatisfy(row -> assertThat(row.getInt(1)).isEqualTo(MATCHING_SCORE));

            // Candidate comes from SSTable #1; the non-indexed update in SSTable #2 must win after reconciliation.
            Row keep = findById(matches, keepId);
            assertThat(keep.getString(2)).isEqualTo("updated");

            // Candidate exists only in SSTable #2, proving candidates from separate SSTables are globally merged.
            Row newMatch = findById(matches, newMatchId);
            assertThat(newMatch.getString(2)).isEqualTo("new-match");

            // deleteId is an old SAI hit with a newer tombstone. changeId is an old score=777 hit whose newer value is
            // score=778. The latter is intentionally left to Spark's residual predicate after Cassandra reconciliation.
            assertThat(actualMatchingIds).doesNotContain(deleteId, changeId);

            // Background rows have much larger payloads than matching rows. A real SAI-pruned read therefore reads
            // substantially less Data.db data; a missing/corrupt SAI that takes the fail-open full-scan path reads
            // approximately the baseline amount and fails this assertion.
            assertThat(saiReadBytes)
            .as("SAI should substantially reduce Data.db reads; fullScanBytes=%s saiReadBytes=%s",
                fullScanBytes, saiReadBytes)
            .isGreaterThan(0)
            .isLessThan(fullScanBytes * 7 / 10);
        }
        finally
        {
            strategy.reset(CassandraDataLayer.class);
            TEST_STATS.reset();
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
        createTestTable(TABLE,
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

        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS score_sai "
                                          + "ON %s (score) USING 'sai';", TABLE),
                            ConsistencyLevel.ALL);

        // Cassandra stores partitions in token order. Pick 1100 adjacent keys in that order so the indexed Data.db
        // reads cover a compact physical region instead of being scattered throughout the SSTable. This makes the
        // physical-read assertion much less sensitive to buffering and SSTable format details.
        List<Integer> idsByToken = IntStream.range(0, TOTAL_ROWS)
                                            .boxed()
                                            .sorted(Comparator.comparing(this::tokenForId))
                                            .collect(Collectors.toList());
        int matchingStart = (TOTAL_ROWS - MATCHING_ROWS) / 2;
        List<Integer> initialMatchingIds = idsByToken.subList(matchingStart, matchingStart + MATCHING_ROWS);
        Set<Integer> initialMatchingIdSet = new HashSet<>(initialMatchingIds);

        // Candidate 1023 is the last key of batch #1 and candidate 1024 is the first key of batch #2.
        // Put reconciliation-sensitive rows exactly around that boundary.
        keepId = initialMatchingIds.get(16);
        deleteId = initialMatchingIds.get(SAI_RECONCILIATION_BATCH_SIZE - 1);
        changeId = initialMatchingIds.get(SAI_RECONCILIATION_BATCH_SIZE);
        newMatchId = idsByToken.get(matchingStart + MATCHING_ROWS);

        expectedMatchingIds.clear();
        expectedMatchingIds.addAll(initialMatchingIds);
        expectedMatchingIds.remove(deleteId);
        expectedMatchingIds.remove(changeId);
        expectedMatchingIds.add(newMatchId);

        // SSTable #1: 1100 SAI matches plus large non-matching rows used for the physical-I/O baseline.
        for (int id = 0; id < TOTAL_ROWS; id++)
        {
            boolean matches = initialMatchingIdSet.contains(id);
            insert(coordinator,
                   id,
                   matches ? MATCHING_SCORE : 1,
                   matches ? "original" : "background",
                   matches ? MATCHING_PAYLOAD : BACKGROUND_PAYLOAD);
        }
        flushTable();

        // SSTable #2 exercises why candidate keys must be reconciled against every selected SSTable:
        // - keepId: old SAI hit, newer non-indexed value must win;
        // - deleteId: old SAI hit, newer partition tombstone must win;
        // - changeId: old SAI hit, newer indexed value no longer matches;
        // - newMatchId: SAI hit exists only in the newer SSTable.
        coordinator.execute(String.format("UPDATE %s SET state = 'updated' WHERE id = %d", TABLE, keepId),
                            ConsistencyLevel.ALL);
        coordinator.execute(String.format("DELETE FROM %s WHERE id = %d", TABLE, deleteId),
                            ConsistencyLevel.ALL);
        coordinator.execute(String.format("UPDATE %s SET score = %d, state = 'changed' WHERE id = %d",
                                          TABLE, CHANGED_SCORE, changeId),
                            ConsistencyLevel.ALL);
        insert(coordinator, newMatchId, MATCHING_SCORE, "new-match", MATCHING_PAYLOAD);
        flushTable();
    }

    private BigInteger tokenForId(int id)
    {
        ByteBuffer key = ByteBuffer.allocate(Integer.BYTES);
        key.putInt(id);
        key.flip();
        return getOrCreateBridge().hash(Partitioner.Murmur3Partitioner, key);
    }

    private static Row findById(List<Row> rows, int id)
    {
        return rows.stream()
                   .filter(row -> row.getInt(0) == id)
                   .findFirst()
                   .orElseThrow(() -> new AssertionError("Expected row with id=" + id));
    }

    private static void insert(ICoordinator coordinator, int id, int score, String state, String payload)
    {
        coordinator.execute(String.format("INSERT INTO %s (id, score, state, payload) VALUES (%d, %d, '%s', '%s')",
                                          TABLE, id, score, state, payload),
                            ConsistencyLevel.ALL);
    }

    private void flushTable()
    {
        cluster.stream().forEach(instance -> instance.nodetool("flush", TEST_KEYSPACE, TABLE.table()));
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
