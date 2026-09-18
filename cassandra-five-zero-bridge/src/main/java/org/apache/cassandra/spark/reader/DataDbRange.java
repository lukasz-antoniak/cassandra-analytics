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

        List<DataDbRange> merged = new ArrayList<>();
        DataDbRange current = sorted.get(0);
        for (int index = 1; index < sorted.size(); index++)
        {
            DataDbRange next = sorted.get(index);
            if (next.start <= current.end)
            {
                current = new DataDbRange(current.start, Math.max(current.end, next.end));
            }
            else
            {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);
        return Collections.unmodifiableList(merged);
    }
}
