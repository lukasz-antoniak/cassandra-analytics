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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SaiIndexReaderTests
{
    @Test
    public void testCandidateIteratorGloballyMergesAndDeduplicatesSources() throws Exception
    {
        TestCandidateSource first = source(1, 3, 5);
        TestCandidateSource second = source(1, 2, 5, 6);
        TestCandidateSource third = source(2, 4, 6);

        try (SaiIndexReader.CandidatePartitionIterator candidates =
             SaiIndexReader.candidatePartitionIterator(Arrays.asList(first, second, third), 2))
        {
            assertThat(partitionKeys(candidates.nextBatch())).containsExactly(1, 2);
            assertThat(partitionKeys(candidates.nextBatch())).containsExactly(3, 4);
            assertThat(partitionKeys(candidates.nextBatch())).containsExactly(5, 6);
            assertThat(candidates.nextBatch()).isEmpty();
        }

        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
        assertThat(third.closed).isTrue();
    }

    @Test
    public void testCandidateIteratorDeduplicatesSameSourceAcrossBatchBoundaries() throws Exception
    {
        TestCandidateSource source = source(1, 1, 2, 2, 3);

        try (SaiIndexReader.CandidatePartitionIterator candidates =
             SaiIndexReader.candidatePartitionIterator(Collections.singletonList(source), 1))
        {
            assertThat(partitionKeys(candidates.nextBatch())).containsExactly(1);
            assertThat(partitionKeys(candidates.nextBatch())).containsExactly(2);
            assertThat(partitionKeys(candidates.nextBatch())).containsExactly(3);
            assertThat(candidates.nextBatch()).isEmpty();
        }
    }

    @Test
    public void testCandidateIteratorClosesAllSourcesWhenLaterPageFails() throws Exception
    {
        TestCandidateSource first = source(1, 3);
        FailingCandidateSource second = new FailingCandidateSource(Arrays.asList(filter(2), filter(4)), 3);

        SaiIndexReader.CandidatePartitionIterator candidates =
        SaiIndexReader.candidatePartitionIterator(Arrays.asList(first, second), 2);

        assertThat(partitionKeys(candidates.nextBatch())).containsExactly(1, 2);
        assertThatThrownBy(candidates::nextBatch)
        .isInstanceOf(IOException.class)
        .hasMessageContaining("candidate source failed");

        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
        assertThat(candidates.nextBatch()).isEmpty();
    }

    private static TestCandidateSource source(int... values)
    {
        return new TestCandidateSource(Arrays.stream(values)
                                             .mapToObj(SaiIndexReaderTests::filter)
                                             .collect(Collectors.toList()));
    }

    private static PartitionKeyFilter filter(int value)
    {
        ByteBuffer key = ByteBuffer.allocate(Integer.BYTES);
        key.putInt(value);
        key.flip();
        return PartitionKeyFilter.create(key, BigInteger.valueOf(value));
    }

    private static List<Integer> partitionKeys(List<PartitionKeyFilter> filters)
    {
        return filters.stream().map(filter -> filter.key().duplicate().getInt()).collect(Collectors.toList());
    }

    private static class TestCandidateSource implements SaiIndexReader.CandidateSource
    {
        protected final List<PartitionKeyFilter> candidates;
        protected int position = -1;
        protected boolean closed;

        private TestCandidateSource(List<PartitionKeyFilter> candidates)
        {
            this.candidates = new ArrayList<>(candidates);
        }

        @Override
        public boolean advance() throws IOException
        {
            if (closed)
            {
                return false;
            }
            position++;
            return position < candidates.size();
        }

        @Override
        public PartitionKeyFilter current()
        {
            return candidates.get(position);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static final class FailingCandidateSource extends TestCandidateSource
    {
        private final int failOnAdvance;
        private int advances;

        private FailingCandidateSource(List<PartitionKeyFilter> candidates, int failOnAdvance)
        {
            super(candidates);
            this.failOnAdvance = failOnAdvance;
        }

        @Override
        public boolean advance() throws IOException
        {
            advances++;
            if (advances == failOnAdvance)
            {
                throw new IOException("candidate source failed");
            }
            return super.advance();
        }
    }
}
