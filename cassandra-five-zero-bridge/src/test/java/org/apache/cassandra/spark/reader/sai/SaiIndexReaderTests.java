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
package org.apache.cassandra.spark.reader.sai;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.utils.TokenUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SaiIndexReaderTests
{
    @Test
    void testNoSSTablesProducesSuccessfulEmptyReadPlan()
    {
        Optional<CandidateTokens> result =
        SaiIndexReader.findCandidateTokens(mock(TableMetadata.class),
                                                Collections.emptySet(),
                                                Collections.singletonList(null),
                                                null);

        assertThat(result).isPresent();
        assertThat(result.orElseThrow().isEmpty()).isTrue();
    }

    @Test
    void testNoFiltersProducesSuccessfulEmptyReadPlan()
    {
        Optional<CandidateTokens> result =
        SaiIndexReader.findCandidateTokens(mock(TableMetadata.class),
                                                Collections.singleton(mock(SSTable.class)),
                                                Collections.emptyList(),
                                                null);

        assertThat(result).isPresent();
        assertThat(result.orElseThrow().isEmpty()).isTrue();
    }

    @Test
    void testCollectCandidateTokensConsumesAllMatchesAndDeduplicatesPartitionTokens()
    {
        List<Candidate> candidates = sortedCandidates("alpha", "bravo", "charlie");

        // Two different SAI row matches may point at the same Cassandra partition. The Data.db
        // read plan is token based, so that partition must be represented only once.
        List<PrimaryKey> matches = Arrays.asList(primaryKey(candidates.get(0).partitionKey),
                                                 primaryKey(candidates.get(0).partitionKey),
                                                 primaryKey(candidates.get(1).partitionKey),
                                                 primaryKey(candidates.get(1).partitionKey),
                                                 primaryKey(candidates.get(2).partitionKey));

        CandidateTokens result =
        SaiIndexReader.collectCandidateTokens(matches.iterator(), null, Murmur3Partitioner.instance);

        assertThat(result.isEmpty()).isFalse();
        for (Candidate candidate : candidates)
        {
            assertThat(result.contains(candidate.token)).isTrue();
        }
        assertThat(result.size()).isEqualTo(candidates.size());
    }

    @Test
    void testCollectCandidateTokensAppliesSparkTokenRange()
    {
        List<Candidate> candidates = sortedCandidates("one", "two", "three", "four", "five");
        Candidate firstIncluded = candidates.get(1);
        Candidate lastIncluded = candidates.get(3);
        SparkRangeFilter sparkRangeFilter =
        SparkRangeFilter.create(TokenRange.closed(firstIncluded.token, lastIncluded.token));

        CandidateTokens result =
        SaiIndexReader.collectCandidateTokens(primaryKeys(candidates).iterator(), sparkRangeFilter, Murmur3Partitioner.instance);

        assertThat(result.contains(candidates.get(0).token)).isFalse();
        assertThat(result.contains(firstIncluded.token)).isTrue();
        assertThat(result.contains(candidates.get(2).token)).isTrue();
        assertThat(result.contains(lastIncluded.token)).isTrue();
        assertThat(result.contains(candidates.get(4).token)).isFalse();
        assertThat(result.size()).isEqualTo(3);
    }

    @Test
    void testCollectCandidateTokensReturnsEmptyWhenSparkRangeRejectsEveryMatch()
    {
        List<Candidate> candidates = sortedCandidates("red", "green", "blue");
        BigInteger upper = candidates.get(0).token.subtract(BigInteger.ONE);
        SparkRangeFilter sparkRangeFilter =
        SparkRangeFilter.create(TokenRange.singleton(upper));

        CandidateTokens result =
        SaiIndexReader.collectCandidateTokens(primaryKeys(candidates).iterator(), sparkRangeFilter, Murmur3Partitioner.instance);

        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void testCollectCandidateTokensRejectsOutOfOrderNativeResults()
    {
        List<Candidate> candidates = sortedCandidates("left", "middle", "right");
        List<PrimaryKey> outOfOrder = Arrays.asList(primaryKey(candidates.get(1).partitionKey),
                                                    primaryKey(candidates.get(0).partitionKey));

        assertThatThrownBy(() -> SaiIndexReader.collectCandidateTokens(outOfOrder.iterator(), null, Murmur3Partitioner.instance))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sorted order");
    }

    private static List<Candidate> sortedCandidates(String... keys)
    {
        List<Candidate> candidates = new ArrayList<>(keys.length);
        for (String key : keys)
        {
            ByteBuffer bytes = ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
            DecoratedKey partitionKey = Murmur3Partitioner.instance.decorateKey(bytes);
            candidates.add(new Candidate(partitionKey,
                                         TokenUtils.tokenToBigInteger(partitionKey.getToken())));
        }
        candidates.sort(Comparator.comparing(candidate -> candidate.token));
        return candidates;
    }

    private static List<PrimaryKey> primaryKeys(List<Candidate> candidates)
    {
        List<PrimaryKey> keys = new ArrayList<>(candidates.size());
        for (Candidate candidate : candidates)
        {
            keys.add(primaryKey(candidate.partitionKey));
        }
        return keys;
    }

    private static PrimaryKey primaryKey(DecoratedKey partitionKey)
    {
        PrimaryKey primaryKey = mock(PrimaryKey.class);
        when(primaryKey.partitionKey()).thenReturn(partitionKey);
        return primaryKey;
    }

    private static final class Candidate
    {
        private final DecoratedKey partitionKey;
        private final BigInteger token;

        private Candidate(DecoratedKey partitionKey, BigInteger token)
        {
            this.partitionKey = partitionKey;
            this.token = token;
        }
    }
}
