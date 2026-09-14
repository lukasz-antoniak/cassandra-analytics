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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Streams rows from a sequence of normal compaction scanners, opening one scanner for each
 * bounded batch of SAI-selected partition keys. The SAI iterator remains open across batches;
 * only Data.db readers are reopened for each batch.
 */
public final class BatchedCompactionStreamScanner implements StreamScanner<RowData>
{
    @FunctionalInterface
    public interface ScannerFactory
    {
        @NotNull
        StreamScanner<RowData> open(@NotNull List<PartitionKeyFilter> partitionKeyFilters) throws IOException;
    }

    @NotNull
    private final PartitionKeyBatchIterator candidatePartitions;
    @NotNull
    private final ScannerFactory scannerFactory;
    @Nullable
    private StreamScanner<RowData> currentScanner;
    private boolean closed;

    public BatchedCompactionStreamScanner(@NotNull PartitionKeyBatchIterator candidatePartitions,
                                          @NotNull ScannerFactory scannerFactory)
    {
        this.candidatePartitions = candidatePartitions;
        this.scannerFactory = scannerFactory;
    }

    @Override
    public RowData data()
    {
        return currentScanner == null ? null : currentScanner.data();
    }

    @Override
    public boolean next() throws IOException
    {
        if (closed)
        {
            return false;
        }

        try
        {
            while (true)
            {
                if (currentScanner != null)
                {
                    if (currentScanner.next())
                    {
                        return true;
                    }
                    closeCurrentScanner();
                }

                List<PartitionKeyFilter> batch = candidatePartitions.nextBatch();
                if (batch.isEmpty())
                {
                    close();
                    return false;
                }
                currentScanner = scannerFactory.open(batch);
            }
        }
        catch (IOException | RuntimeException exception)
        {
            // Once any batch may have emitted rows, falling back to a full scan could duplicate output.
            // Close all SAI/Data.db resources and let Spark retry the task instead.
            close();
            throw exception;
        }
    }

    @Override
    public void advanceToNextColumn() throws IOException
    {
        if (currentScanner != null)
        {
            currentScanner.advanceToNextColumn();
        }
    }

    @Override
    public boolean hasMoreColumns()
    {
        return currentScanner != null && currentScanner.hasMoreColumns();
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        closeCurrentScanner();
        closeQuietly(candidatePartitions);
    }

    private void closeCurrentScanner()
    {
        closeQuietly(currentScanner);
        currentScanner = null;
    }

    private static void closeQuietly(@Nullable Closeable closeable)
    {
        if (closeable == null)
        {
            return;
        }
        try
        {
            closeable.close();
        }
        catch (IOException ignore)
        {
            // Best-effort cleanup. A read error, if any, has already been surfaced by next().
        }
    }
}
