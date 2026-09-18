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
package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.TokenRange;
import org.assertj.core.groups.Tuple;

import static org.assertj.core.api.Assertions.assertThat;

class CandidateTokenRangesTests
{
    @Test
    void testBuilderDeduplicatesAndCompactsAdjacentTokens()
    {
        CandidateTokenRanges.Builder builder = CandidateTokenRanges.builder();
        builder.add(BigInteger.TEN);
        builder.add(BigInteger.TEN);
        builder.add(BigInteger.valueOf(11));
        builder.add(BigInteger.valueOf(20));
        builder.add(BigInteger.valueOf(21));
        builder.add(BigInteger.valueOf(23));

        CandidateTokenRanges ranges = builder.build();

        assertThat(ranges.ranges()).containsExactly(TokenRange.closed(BigInteger.TEN, BigInteger.valueOf(11)),
                                                    TokenRange.closed(BigInteger.valueOf(20), BigInteger.valueOf(21)),
                                                    TokenRange.singleton(BigInteger.valueOf(23)));
        assertThat(ranges.contains(BigInteger.valueOf(10))).isTrue();
        assertThat(ranges.contains(BigInteger.valueOf(12))).isFalse();
        assertThat(ranges.contains(BigInteger.valueOf(23))).isTrue();
    }

    @Test
    void testOverlappingUsesSortedSubRange()
    {
        CandidateTokenRanges.Builder builder = CandidateTokenRanges.builder();
        builder.add(BigInteger.ONE);
        builder.add(BigInteger.TEN);
        builder.add(BigInteger.valueOf(20));
        builder.add(BigInteger.valueOf(30));

        CandidateTokenRanges ranges = builder.build();

        assertThat(ranges.overlapping(TokenRange.closed(BigInteger.valueOf(9), BigInteger.valueOf(21))))
                .containsExactly(TokenRange.singleton(BigInteger.TEN),
                                 TokenRange.singleton(BigInteger.valueOf(20)));
    }

    @Test
    void testDataDbRangesMergePhysicalSiblings()
    {
        assertThat(DataDbRange.mergeAdjacent(Arrays.asList(new DataDbRange(10, 20),
                                                           new DataDbRange(20, 30),
                                                           new DataDbRange(40, 50))))
                .extracting(DataDbRange::start, DataDbRange::end)
                .containsExactly(Tuple.tuple(10L, 30L),
                                 Tuple.tuple(40L, 50L));
    }
}
