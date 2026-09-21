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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.jetbrains.annotations.NotNull;

/** An uncompressed Data.db byte range in the form {@code [start, end)}. */
public final class DataDbRange
{
    /**
     * Candidate reads may include a small amount of non-matching data to avoid another physical range/seek.
     * Exact candidate-token filtering still happens while Data.db is scanned, so this only affects I/O volume.
     */
    public static final long DEFAULT_MAX_COALESCE_GAP_BYTES = 64L * 1024L;

    private final long start;
    private final long end;

    public DataDbRange(long start, long end)
    {
        if (start < 0 || end < start)
        {
            throw new IllegalArgumentException("Invalid Data.db range [" + start + ", " + end + ')');
        }
        this.start = start;
        this.end = end;
    }

    public long start()
    {
        return start;
    }

    public long end()
    {
        return end;
    }

    /**
     * Merge overlapping and physically adjacent Data.db ranges.
     *
     * <p>This is intentionally byte-based rather than token-distance-based. Two SAI tokens can be far apart in
     * Murmur3 token space while still referring to consecutive partitions in a particular SSTable.</p>
     */
    @NotNull
    public static List<DataDbRange> mergeAdjacent(@NotNull Collection<DataDbRange> input)
    {
        if (input.isEmpty())
        {
            return Collections.emptyList();
        }

        List<DataDbRange> sorted = new ArrayList<>(input);
        sorted.sort(Comparator.comparingLong(DataDbRange::start));
        Accumulator accumulator = new Accumulator(0L);
        for (DataDbRange range : sorted)
        {
            accumulator.add(range.start, range.end);
        }
        return accumulator.build();
    }

    /**
     * Incrementally coalesces already sorted physical Data.db ranges without retaining a temporary range per hit.
     */
    public static final class Accumulator
    {
        private final long maxGapBytes;
        private final List<DataDbRange> ranges = new ArrayList<>();
        private long currentStart = -1L;
        private long currentEnd = -1L;

        public Accumulator(long maxGapBytes)
        {
            if (maxGapBytes < 0)
            {
                throw new IllegalArgumentException("maxGapBytes must be non-negative");
            }
            this.maxGapBytes = maxGapBytes;
        }

        public void add(long start, long end)
        {
            if (start < 0 || end < start)
            {
                throw new IllegalArgumentException("Invalid Data.db range [" + start + ", " + end + ')');
            }

            if (currentStart < 0)
            {
                currentStart = start;
                currentEnd = end;
                return;
            }

            if (start < currentStart)
            {
                throw new IllegalArgumentException("Data.db ranges must be supplied in sorted order");
            }

            long gap = start <= currentEnd ? 0L : start - currentEnd;
            if (gap <= maxGapBytes)
            {
                currentEnd = Math.max(currentEnd, end);
            }
            else
            {
                flush();
                currentStart = start;
                currentEnd = end;
            }
        }

        @NotNull
        public List<DataDbRange> build()
        {
            flush();
            if (ranges.isEmpty())
            {
                return Collections.emptyList();
            }
            return Collections.unmodifiableList(new ArrayList<>(ranges));
        }

        private void flush()
        {
            if (currentStart >= 0)
            {
                ranges.add(new DataDbRange(currentStart, currentEnd));
                currentStart = -1L;
                currentEnd = -1L;
            }
        }
    }
}
