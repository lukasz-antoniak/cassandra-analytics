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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BatchedCompactionStreamScannerTests
{
    @Test
    public void testStreamsAllCandidateBatchesAndSwitchesRowData() throws IOException
    {
        TestBatchIterator batches = new TestBatchIterator(Arrays.asList(batch(1, 2), batch(3, 4), batch(5)));
        List<RowData> rowDataInstances = new ArrayList<>();
        List<TestScanner> openedScanners = new ArrayList<>();

        BatchedCompactionStreamScanner scanner = new BatchedCompactionStreamScanner(batches, batch -> {
            TestScanner batchScanner = new TestScanner(batch);
            openedScanners.add(batchScanner);
            rowDataInstances.add(batchScanner.data());
            return batchScanner;
        });

        List<BigInteger> tokens = new ArrayList<>();
        List<RowData> observedRowData = new ArrayList<>();
        while (scanner.next())
        {
            observedRowData.add(scanner.data());
            tokens.add(scanner.data().getToken());
        }

        assertThat(tokens).containsExactly(BigInteger.ONE,
                                           BigInteger.valueOf(2),
                                           BigInteger.valueOf(3),
                                           BigInteger.valueOf(4),
                                           BigInteger.valueOf(5));
        assertThat(rowDataInstances).hasSize(3);
        assertThat(observedRowData.get(0)).isSameAs(rowDataInstances.get(0));
        assertThat(observedRowData.get(2)).isSameAs(rowDataInstances.get(1));
        assertThat(observedRowData.get(4)).isSameAs(rowDataInstances.get(2));
        assertThat(batches.closed).isTrue();
        assertThat(openedScanners).allMatch(batchScanner -> batchScanner.closed);
        assertThat(scanner.next()).isFalse();
    }

    @Test
    public void testClosesResourcesWhenCandidateStreamingFails() throws IOException
    {
        FailingBatchIterator batches = new FailingBatchIterator();
        List<TestScanner> openedScanners = new ArrayList<>();
        BatchedCompactionStreamScanner scanner = new BatchedCompactionStreamScanner(batches, batch -> {
            TestScanner batchScanner = new TestScanner(batch);
            openedScanners.add(batchScanner);
            return batchScanner;
        });

        assertThat(scanner.next()).isTrue();
        assertThat(scanner.data().getToken()).isEqualTo(BigInteger.ONE);
        assertThatThrownBy(scanner::next)
        .isInstanceOf(IOException.class)
        .hasMessageContaining("candidate stream failed");

        assertThat(openedScanners).hasSize(1);
        assertThat(openedScanners.get(0).closed).isTrue();
        assertThat(batches.closed).isTrue();
        assertThat(scanner.next()).isFalse();
    }

    private static List<PartitionKeyFilter> batch(int... values)
    {
        return Arrays.stream(values).mapToObj(BatchedCompactionStreamScannerTests::partitionKeyFilter).collect(Collectors.toList());
    }

    private static PartitionKeyFilter partitionKeyFilter(int value)
    {
        ByteBuffer key = ByteBuffer.allocate(Integer.BYTES);
        key.putInt(value);
        key.flip();
        return PartitionKeyFilter.create(key, BigInteger.valueOf(value));
    }

    private static final class TestBatchIterator implements PartitionKeyBatchIterator
    {
        private final Deque<List<PartitionKeyFilter>> batches;
        private boolean closed;

        private TestBatchIterator(List<List<PartitionKeyFilter>> batches)
        {
            this.batches = new ArrayDeque<>(batches);
        }

        @Override
        public List<PartitionKeyFilter> nextBatch()
        {
            return batches.isEmpty() ? Collections.emptyList() : batches.removeFirst();
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static final class FailingBatchIterator implements PartitionKeyBatchIterator
    {
        private int calls;
        private boolean closed;

        @Override
        public List<PartitionKeyFilter> nextBatch() throws IOException
        {
            calls++;
            if (calls == 1)
            {
                return batch(1);
            }
            throw new IOException("candidate stream failed");
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static final class TestScanner implements StreamScanner<RowData>
    {
        private final List<PartitionKeyFilter> filters;
        private final RowData rowData = new RowData();
        private int index;
        private boolean closed;

        private TestScanner(List<PartitionKeyFilter> filters)
        {
            this.filters = filters;
        }

        @Override
        public RowData data()
        {
            return rowData;
        }

        @Override
        public boolean next()
        {
            if (index >= filters.size())
            {
                return false;
            }

            PartitionKeyFilter filter = filters.get(index++);
            rowData.setPartitionKeyCopy(filter.key(), filter.token());
            return true;
        }

        @Override
        public void advanceToNextColumn()
        {
        }

        @Override
        public boolean hasMoreColumns()
        {
            return false;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
