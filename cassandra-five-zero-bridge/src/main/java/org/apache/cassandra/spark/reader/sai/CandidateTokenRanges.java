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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.bridge.TokenRange;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable, sorted and non-overlapping token ranges returned by the SAI pruning phase.
 *
 * <p>The builder consumes SAI tokens in sorted order and deduplicates repeated tokens. Numerically adjacent
 * tokens are compacted immediately. Later, each SSTable translates these token ranges into Data.db byte ranges;
 * byte ranges are compacted again when matching partitions are physically adjacent in that SSTable.</p>
 */
public final class CandidateTokenRanges
{
    private static final CandidateTokenRanges EMPTY = new CandidateTokenRanges(Collections.emptyList());

    @NotNull
    private final List<TokenRange> ranges;

    private CandidateTokenRanges(@NotNull List<TokenRange> ranges)
    {
        this.ranges = Collections.unmodifiableList(new ArrayList<>(ranges));
    }

    @NotNull
    public static CandidateTokenRanges empty()
    {
        return EMPTY;
    }

    @NotNull
    public static Builder builder()
    {
        return new Builder();
    }

    public boolean isEmpty()
    {
        return ranges.isEmpty();
    }

    @NotNull
    public List<TokenRange> ranges()
    {
        return ranges;
    }

    /**
     * Returns a view of the ranges which overlap the supplied SSTable token range.
     */
    @NotNull
    public List<TokenRange> overlapping(@NotNull TokenRange sstableRange)
    {
        if (ranges.isEmpty())
        {
            return Collections.emptyList();
        }

        BigInteger first = sstableRange.firstEnclosedValue();
        BigInteger last = sstableRange.upperEndpoint();

        int low = 0;
        int high = ranges.size();
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            if (ranges.get(mid).upperEndpoint().compareTo(first) < 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        int start = low;
        while (low < ranges.size() && ranges.get(low).firstEnclosedValue().compareTo(last) <= 0)
        {
            low++;
        }
        return start == low ? Collections.emptyList() : ranges.subList(start, low);
    }

    public boolean contains(@NotNull BigInteger token)
    {
        int low = 0;
        int high = ranges.size() - 1;
        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            TokenRange range = ranges.get(mid);
            if (token.compareTo(range.lowerEndpoint()) <= 0)
            {
                high = mid - 1;
            }
            else if (token.compareTo(range.upperEndpoint()) > 0)
            {
                low = mid + 1;
            }
            else
            {
                return true;
            }
        }
        return false;
    }

    public static final class Builder
    {
        private final List<TokenRange> ranges = new ArrayList<>();
        private BigInteger first;
        private BigInteger last;

        private Builder()
        {
        }

        public void add(@NotNull BigInteger token)
        {
            if (first == null)
            {
                first = token;
                last = token;
                return;
            }

            int comparison = token.compareTo(last);
            if (comparison < 0)
            {
                throw new IllegalArgumentException("SAI tokens must be supplied in sorted order");
            }
            if (comparison == 0)
            {
                return;
            }

            if (token.equals(last.add(BigInteger.ONE)))
            {
                last = token;
                return;
            }

            flush();
            first = token;
            last = token;
        }

        @NotNull
        public CandidateTokenRanges build()
        {
            flush();
            return ranges.isEmpty() ? CandidateTokenRanges.empty() : new CandidateTokenRanges(ranges);
        }

        private void flush()
        {
            if (first != null)
            {
                ranges.add(TokenRange.closed(first, last));
                first = null;
                last = null;
            }
        }
    }
}
