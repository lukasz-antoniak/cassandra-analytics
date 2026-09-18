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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.utils.TokenUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Opens Cassandra 5 SAI components, executes the query plan once, and materializes the resulting candidate tokens
 * as compact ranges.
 *
 * <p>For every indexed predicate, matches are first UNIONed across all SSTables. The resulting global predicate
 * streams are then composed using Cassandra's native SAI iterators: AND becomes intersection and OR becomes union.
 * This ordering is important for correctness: values contributing to a logically reconciled row may live in
 * different SSTables. Applying boolean composition inside each SSTable could therefore create false negatives.</p>
 *
 * <p>SAI is a read-planning phase only. The native iterator is fully consumed before Data.db scanning starts, then
 * all SAI resources are closed. The resulting token ranges are passed to every participating SSTable so the normal
 * Cassandra compaction/reconciliation path can still apply newer values, TTLs and tombstones. Spark keeps the
 * original predicates as residual filters, so stale index entries can only create false positives.</p>
 */
public final class SaiIndexReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SaiIndexReader.class);
    private static final long NO_TIMEOUT_MILLIS = Long.MAX_VALUE;

    private SaiIndexReader()
    {
        throw new IllegalStateException("Static utility class");
    }

    /**
     * Executes all SAI predicates once and returns the sorted candidate tokens as compact ranges.
     *
     * @return empty Optional when SAI cannot safely be used and the caller should fall back to the ordinary SSTable
     *         scan; an empty {@link CandidateTokenRanges} means SAI executed successfully and found no candidates
     */
    @NotNull
    public static Optional<CandidateTokenRanges> findCandidateTokenRanges(@NotNull TableMetadata metadata,
                                                                          @NotNull Set<SSTable> sstables,
                                                                          @NotNull List<SaiFilter> filters,
                                                                          @Nullable SparkRangeFilter sparkRangeFilter)
    {
        if (sstables.isEmpty() || filters.isEmpty())
        {
            return Optional.of(CandidateTokenRanges.empty());
        }

        ResourceGroup resources = new ResourceGroup();
        try
        {
            SaiQueryPlanner.Plan plan = SaiQueryPlanner.plan(metadata, filters);
            if (plan == null)
            {
                return Optional.empty();
            }

            List<SaiQueryPlanner.IndexPlan> resourcePlans = plan.resourcePlans();

            List<SSTableResources> sstableResources = new ArrayList<>(sstables.size());
            for (SSTable sstable : sstables)
            {
                SSTableResources opened = SSTableResources.open(metadata, sstable, resourcePlans);
                resources.add(opened);
                sstableResources.add(opened);
            }

            QueryContext queryContext = new QueryContext(null, NO_TIMEOUT_MILLIS);
            try (KeyRangeIterator finalMatches = executePlan(plan, sstableResources, queryContext))
            {
                finalMatches.setOnClose(resources::close);
                CandidateTokenRanges.Builder candidates = CandidateTokenRanges.builder();
                while (finalMatches.hasNext())
                {
                    PrimaryKey primaryKey = finalMatches.next();
                    DecoratedKey partitionKey = primaryKey.partitionKey();
                    BigInteger token = TokenUtils.tokenToBigInteger(partitionKey.getToken());
                    if (sparkRangeFilter == null || !sparkRangeFilter.skipPartition(token))
                    {
                        candidates.add(token);
                    }
                }
                return Optional.of(candidates.build());
            }
        }
        catch (Throwable throwable)
        {
            resources.close();
            // No Data.db rows have been emitted yet, so every SAI failure can safely fail open to a normal scan.
            LOGGER.warn("Unable to use SAI for SSTable pruning; falling back to normal SSTable scan", throwable);
            return Optional.empty();
        }
    }

    @NotNull
    private static KeyRangeIterator executePlan(@NotNull SaiQueryPlanner.Plan plan,
                                                @NotNull List<SSTableResources> sstableResources,
                                                @NotNull QueryContext queryContext) throws IOException
    {
        if (plan.isPredicate())
        {
            List<KeyRangeIterator> perSSTableMatches = new ArrayList<>(sstableResources.size());
            try
            {
                for (SSTableResources resource : sstableResources)
                {
                    perSSTableMatches.add(resource.search(plan.predicate(), queryContext));
                }
                // Always UNION a predicate across SSTables before composing boolean operators. A logical row may be
                // assembled from values written in different SSTables, so per-SSTable boolean composition can create
                // false negatives.
                return KeyRangeUnionIterator.build(perSSTableMatches);
            }
            catch (Throwable throwable)
            {
                closeAll(perSSTableMatches);
                rethrowSearchFailure("Unable to execute SAI predicate", throwable);
                throw new AssertionError("unreachable");
            }
        }

        List<KeyRangeIterator> children = new ArrayList<>(plan.children().size());
        try
        {
            for (SaiQueryPlanner.Plan child : plan.children())
            {
                children.add(executePlan(child, sstableResources, queryContext));
            }
            if (plan.isOr())
            {
                return KeyRangeUnionIterator.build(children);
            }

            KeyRangeIntersectionIterator.Builder intersection = KeyRangeIntersectionIterator.builder(children.size(), 0);
            for (KeyRangeIterator child : children)
            {
                intersection.add(child);
            }
            return intersection.build();
        }
        catch (Throwable throwable)
        {
            closeAll(children);
            rethrowSearchFailure("Unable to execute SAI boolean plan", throwable);
            throw new AssertionError("unreachable");
        }
    }

    private static void rethrowSearchFailure(String message, Throwable throwable) throws IOException
    {
        if (throwable instanceof IOException)
        {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException)
        {
            throw (RuntimeException) throwable;
        }
        if (throwable instanceof Error)
        {
            throw (Error) throwable;
        }
        throw new IOException(message, throwable);
    }

    /** Holds all open native SAI resources for one SSTable. */
    private static final class SSTableResources implements Closeable
    {
        private final Descriptor descriptor;
        private final IndexDescriptor indexDescriptor;
        private final PrimaryKeyMap.Factory primaryKeyMapFactory;
        private final Map<StorageAttachedIndex, OpenColumnIndex> columns = new HashMap<>();
        private boolean closed;

        private SSTableResources(Descriptor descriptor,
                                 IndexDescriptor indexDescriptor,
                                 PrimaryKeyMap.Factory primaryKeyMapFactory)
        {
            this.descriptor = descriptor;
            this.indexDescriptor = indexDescriptor;
            this.primaryKeyMapFactory = primaryKeyMapFactory;
        }

        @NotNull
        static SSTableResources open(@NotNull TableMetadata metadata,
                                     @NotNull SSTable sstable,
                                     @NotNull List<SaiQueryPlanner.IndexPlan> plans) throws IOException
        {
            if (sstable.customComponentNames().isEmpty())
            {
                throw new IOException("SSTable has no SAI components: " + sstable.getDataFileName());
            }

            SSTableResources resources = null;
            try
            {
                // Descriptor is still needed for Cassandra's SAI naming/ID logic, but this
                // path is only an identifier. No file or directory is created or opened.
                org.apache.cassandra.io.util.File dataFile = new org.apache.cassandra.io.util.File(".", sstable.getDataFileName());
                Descriptor descriptor = Descriptor.fromFileWithComponent(dataFile, metadata.keyspace, metadata.name).left;
                IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator,
                                                                         new SSTableIndexFileAccess(sstable));

                if (!indexDescriptor.isPerSSTableIndexBuildComplete())
                {
                    throw new IOException("Incomplete per-SSTable SAI components: " + sstable.getDataFileName());
                }
                for (SaiQueryPlanner.IndexPlan plan : plans)
                {
                    if (!indexDescriptor.isPerColumnIndexBuildComplete(plan.index().identifier()))
                    {
                        throw new IOException("Incomplete SAI components for " + plan.index().identifier()
                                              + " in " + sstable.getDataFileName());
                    }
                }

                resources = new SSTableResources(descriptor,
                                                 indexDescriptor,
                                                 indexDescriptor.newPrimaryKeyMapFactory(null));
                for (SaiQueryPlanner.IndexPlan plan : plans)
                {
                    resources.openColumn(plan);
                }
                return resources;
            }
            catch (Throwable throwable)
            {
                if (resources != null)
                {
                    resources.close();
                }
                if (throwable instanceof IOException)
                {
                    throw (IOException) throwable;
                }
                throw new IOException("Unable to open SAI components for " + sstable.getDataFileName(), throwable);
            }
        }

        private void openColumn(SaiQueryPlanner.IndexPlan plan) throws IOException
        {
            StorageAttachedIndex index = plan.index();
            if (indexDescriptor.isIndexEmpty(index.termType(), index.identifier()))
            {
                columns.put(index, OpenColumnIndex.empty());
                return;
            }

            PerColumnIndexFiles indexFiles = new PerColumnIndexFiles(indexDescriptor,
                                                                     index.termType(),
                                                                     index.identifier());
            OpenColumnIndex column = new OpenColumnIndex(indexFiles);
            try
            {
                MetadataSource metadataSource = MetadataSource.loadColumnMetadata(indexDescriptor, index.identifier());
                List<SegmentMetadata> segments = SegmentMetadata.load(metadataSource, indexDescriptor.primaryKeyFactory);
                for (SegmentMetadata segment : segments)
                {
                    column.segments.add(new OpenSegment(segment,
                                                        IndexSegmentSearcher.open(primaryKeyMapFactory,
                                                                                  descriptor.id,
                                                                                  indexFiles,
                                                                                  segment,
                                                                                  index)));
                }
                columns.put(index, column);
            }
            catch (Throwable throwable)
            {
                column.close();
                if (throwable instanceof IOException)
                {
                    throw (IOException) throwable;
                }
                throw new IOException("Unable to open SAI column " + index.identifier(), throwable);
            }
        }

        @NotNull
        KeyRangeIterator search(@NotNull SaiQueryPlanner.IndexPlan plan, @NotNull QueryContext queryContext) throws IOException
        {
            OpenColumnIndex column = columns.get(plan.index());
            if (column == null || column.segments.isEmpty())
            {
                return KeyRangeIterator.empty();
            }

            List<KeyRangeIterator> segmentMatches = new ArrayList<>(column.segments.size());
            try
            {
                for (OpenSegment segment : column.segments)
                {
                    Bounds<PartitionPosition> keyRange = new Bounds<>(segment.metadata.minKey.partitionKey(),
                                                                      segment.metadata.maxKey.partitionKey());
                    segmentMatches.add(segment.searcher.search(plan.expression(), keyRange, queryContext));
                }
                return KeyRangeUnionIterator.build(segmentMatches);
            }
            catch (Throwable throwable)
            {
                closeAll(segmentMatches);
                if (throwable instanceof IOException)
                {
                    throw (IOException) throwable;
                }
                throw new IOException("Unable to search SAI column " + plan.index().identifier(), throwable);
            }
        }

        @Override
        public void close()
        {
            if (closed)
            {
                return;
            }
            closed = true;
            for (OpenColumnIndex column : columns.values())
            {
                closeQuietly(column);
            }
            columns.clear();
            closeQuietly(primaryKeyMapFactory);
        }
    }

    private static final class OpenColumnIndex implements Closeable
    {
        @Nullable
        private final PerColumnIndexFiles indexFiles;
        private final List<OpenSegment> segments = new ArrayList<>();
        private boolean closed;

        private OpenColumnIndex(@Nullable PerColumnIndexFiles indexFiles)
        {
            this.indexFiles = indexFiles;
        }

        static OpenColumnIndex empty()
        {
            return new OpenColumnIndex(null);
        }

        @Override
        public void close()
        {
            if (closed)
            {
                return;
            }
            closed = true;
            for (OpenSegment segment : segments)
            {
                closeQuietly(segment.searcher);
            }
            segments.clear();
            closeQuietly(indexFiles);
        }
    }

    private static final class OpenSegment
    {
        private final SegmentMetadata metadata;
        private final IndexSegmentSearcher searcher;

        private OpenSegment(SegmentMetadata metadata, IndexSegmentSearcher searcher)
        {
            this.metadata = metadata;
            this.searcher = searcher;
        }
    }

    /** Owns all per-SSTable resources and closes them exactly once. */
    private static final class ResourceGroup implements Closeable
    {
        private final List<SSTableResources> resources = new ArrayList<>();
        private boolean closed;

        void add(SSTableResources resource)
        {
            resources.add(resource);
        }

        @Override
        public void close()
        {
            if (closed)
            {
                return;
            }
            closed = true;
            for (SSTableResources resource : resources)
            {
                closeQuietly(resource);
            }
            resources.clear();
        }
    }

    private static void closeAll(List<? extends Closeable> closeables)
    {
        for (Closeable closeable : closeables)
        {
            closeQuietly(closeable);
        }
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
        catch (IOException exception)
        {
            LOGGER.debug("Unable to close SAI resource", exception);
        }
    }
}
