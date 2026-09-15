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
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SaiIndex;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.utils.TokenUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Opens Cassandra 5 SAI components once for the lifetime of a Spark read partition and streams matching
 * primary keys from Cassandra's native SAI iterators.
 *
 * <p>For each indexed column, matches are first UNIONed across all SSTables. The resulting per-column streams are
 * then intersected. This ordering is important for correctness: values contributing to a logically reconciled row
 * may live in different SSTables. Intersecting inside each SSTable could therefore create false negatives.</p>
 *
 * <p>SAI is used only to identify candidate partitions. Each emitted partition batch must still be read from all
 * participating Data.db SSTables and passed through the normal Cassandra compaction/reconciliation path. Spark keeps
 * the original predicates as residual filters, so stale index entries caused by updates, TTLs, or tombstones can
 * only create false positives, never incorrect returned rows.</p>
 */
public final class SaiIndexReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SaiIndexReader.class);
    private static final Map<String, StorageAttachedIndex> INDEXES = new ConcurrentHashMap<>();
    private static final long NO_TIMEOUT_MILLIS = Long.MAX_VALUE;

    private SaiIndexReader()
    {
        throw new IllegalStateException("Static utility class");
    }

    /**
     * Opens all SAI resources required by the supplied predicates and returns a lazy, closeable batch iterator.
     *
     * @return empty when SAI cannot safely be used and the caller should fall back to the ordinary SSTable scan
     */
    @NotNull
    public static Optional<PartitionKeyBatchIterator> openCandidatePartitionIterator(@NotNull TableMetadata metadata,
                                                                                     @NotNull Set<SSTable> sstables,
                                                                                     @NotNull List<SaiFilter> filters,
                                                                                     @Nullable SparkRangeFilter sparkRangeFilter,
                                                                                     int batchSize)
    {
        assert batchSize > 0;
        if (sstables.isEmpty() || filters.isEmpty())
        {
            return Optional.of(new EmptyPartitionKeyBatchIterator());
        }

        ResourceGroup resources = new ResourceGroup();
        try
        {
            List<IndexPlan> plans = buildPlans(metadata, filters);
            if (plans.isEmpty())
            {
                return Optional.empty();
            }

            // Open/materialize every SAI component once. The resources remain live until the final result iterator
            // is exhausted/closed, so advancing to the next Data.db batch never reopens SAI or skips prior postings.
            List<SSTableResources> sstableResources = new ArrayList<>(sstables.size());
            for (SSTable sstable : sstables)
            {
                SSTableResources opened = SSTableResources.open(metadata, sstable, plans);
                resources.add(opened);
                sstableResources.add(opened);
            }

            QueryContext queryContext = new QueryContext(null, NO_TIMEOUT_MILLIS);
            List<KeyRangeIterator> perIndexGlobalMatches = new ArrayList<>(plans.size());
            for (IndexPlan plan : plans)
            {
                List<KeyRangeIterator> perSSTableMatches = new ArrayList<>(sstableResources.size());
                for (SSTableResources resource : sstableResources)
                {
                    perSSTableMatches.add(resource.search(plan, queryContext));
                }
                // A logical row can be assembled from different SSTables. UNION the same predicate across SSTables
                // before applying AND with predicates on other columns.
                perIndexGlobalMatches.add(KeyRangeUnionIterator.build(perSSTableMatches));
            }

            KeyRangeIntersectionIterator.Builder intersection = KeyRangeIntersectionIterator.builder(perIndexGlobalMatches.size(),
                                                                                                     0,
                                                                                                     resources::close);
            for (KeyRangeIterator matches : perIndexGlobalMatches)
            {
                intersection.add(matches);
            }
            KeyRangeIterator finalMatches = intersection.build();
            return Optional.of(new CandidatePartitionIterator(finalMatches, resources, sparkRangeFilter, batchSize));
        }
        catch (Throwable throwable)
        {
            resources.close();
            // SAI is an optimization. Missing/corrupt/unsupported index state detected before rows are emitted
            // fails open to the existing full SSTable scan.
            LOGGER.warn("Unable to use SAI for SSTable pruning; falling back to normal SSTable scan", throwable);
            return Optional.empty();
        }
    }

    @NotNull
    private static List<IndexPlan> buildPlans(@NotNull TableMetadata metadata, @NotNull List<SaiFilter> filters)
    {
        Map<SaiIndex, List<SaiFilter>> grouped = new LinkedHashMap<>();
        for (SaiFilter filter : filters)
        {
            grouped.computeIfAbsent(filter.index(), ignored -> new ArrayList<>()).add(filter);
        }

        List<IndexPlan> plans = new ArrayList<>(grouped.size());
        for (Map.Entry<SaiIndex, List<SaiFilter>> entry : grouped.entrySet())
        {
            StorageAttachedIndex index = storageAttachedIndex(metadata, entry.getKey());
            if (!canSearch(index, entry.getValue()))
            {
                return Collections.emptyList();
            }

            Expression expression = Expression.create(index);
            for (SaiFilter filter : entry.getValue())
            {
                expression.add(operator(filter.operator()), index.termType().fromString(filter.value()));
            }
            plans.add(new IndexPlan(index, expression));
        }
        return plans;
    }

    private static boolean canSearch(@NotNull StorageAttachedIndex index, @NotNull List<SaiFilter> filters)
    {
        if (index.hasAnalyzer()
            || index.termType().isVector()
            || index.termType().isNonFrozenCollection()
            || index.termType().isFrozenCollection()
            || index.termType().isComposite())
        {
            return false;
        }

        for (SaiFilter filter : filters)
        {
            if (!index.supportsExpression(index.termType().columnMetadata(), operator(filter.operator())))
            {
                return false;
            }
        }
        return true;
    }

    @NotNull
    private static StorageAttachedIndex storageAttachedIndex(@NotNull TableMetadata metadata, @NotNull SaiIndex saiIndex)
    {
        String cacheKey = metadata.id + ":" + saiIndex.name();
        return INDEXES.computeIfAbsent(cacheKey, ignored -> {
            ColumnFamilyStore cfs = Keyspace.openWithoutSSTables(metadata.keyspace).getColumnFamilyStore(metadata.name);
            Map<String, String> options = new HashMap<>(saiIndex.options());
            options.put(IndexTarget.TARGET_OPTION_NAME, saiIndex.target());
            options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(saiIndex.name(), IndexMetadata.Kind.CUSTOM, options);
            return new StorageAttachedIndex(cfs, indexMetadata);
        });
    }

    @NotNull
    private static Operator operator(@NotNull SaiFilter.Operator operator)
    {
        switch (operator)
        {
            case EQ:
                return Operator.EQ;
            case LT:
                return Operator.LT;
            case LTE:
                return Operator.LTE;
            case GT:
                return Operator.GT;
            case GTE:
                return Operator.GTE;
            default:
                throw new IllegalArgumentException("Unsupported SAI operator: " + operator);
        }
    }

    private static final class IndexPlan
    {
        private final StorageAttachedIndex index;
        private final Expression expression;

        private IndexPlan(StorageAttachedIndex index, Expression expression)
        {
            this.index = index;
            this.expression = expression;
        }
    }

    /** Holds all open native SAI resources for one SSTable. */
    private static final class SSTableResources implements Closeable
    {
        private final Path temporaryDirectory;
        private final Descriptor descriptor;
        private final IndexDescriptor indexDescriptor;
        private final PrimaryKeyMap.Factory primaryKeyMapFactory;
        private final Map<StorageAttachedIndex, OpenColumnIndex> columns = new HashMap<>();
        private boolean closed;

        private SSTableResources(Path temporaryDirectory,
                                 Descriptor descriptor,
                                 IndexDescriptor indexDescriptor,
                                 PrimaryKeyMap.Factory primaryKeyMapFactory)
        {
            this.temporaryDirectory = temporaryDirectory;
            this.descriptor = descriptor;
            this.indexDescriptor = indexDescriptor;
            this.primaryKeyMapFactory = primaryKeyMapFactory;
        }

        @NotNull
        static SSTableResources open(@NotNull TableMetadata metadata,
                                     @NotNull SSTable sstable,
                                     @NotNull List<IndexPlan> plans) throws IOException
        {
            if (sstable.customComponentNames().isEmpty())
            {
                throw new IOException("SSTable has no SAI components: " + sstable.getDataFileName());
            }

            Path temporaryDirectory = Files.createTempDirectory("cassandra-analytics-sai-");
            SSTableResources resources = null;
            try
            {
                org.apache.cassandra.io.util.File dataFile = new org.apache.cassandra.io.util.File(temporaryDirectory.resolve(sstable.getDataFileName()));
                Descriptor descriptor = Descriptor.fromFileWithComponent(dataFile, metadata.keyspace, metadata.name).left;
                IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator);
                materializeIndexComponents(sstable, indexDescriptor, plans);

                if (!indexDescriptor.isPerSSTableIndexBuildComplete())
                {
                    throw new IOException("Incomplete per-SSTable SAI components: " + sstable.getDataFileName());
                }
                for (IndexPlan plan : plans)
                {
                    if (!indexDescriptor.isPerColumnIndexBuildComplete(plan.index.identifier()))
                    {
                        throw new IOException("Incomplete SAI components for " + plan.index.identifier()
                                              + " in " + sstable.getDataFileName());
                    }
                }

                resources = new SSTableResources(temporaryDirectory,
                                                 descriptor,
                                                 indexDescriptor,
                                                 indexDescriptor.newPrimaryKeyMapFactory(null));
                for (IndexPlan plan : plans)
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
                else
                {
                    deleteRecursively(temporaryDirectory);
                }
                if (throwable instanceof IOException)
                {
                    throw (IOException) throwable;
                }
                throw new IOException("Unable to open SAI components for " + sstable.getDataFileName(), throwable);
            }
        }

        private void openColumn(IndexPlan plan) throws IOException
        {
            if (indexDescriptor.isIndexEmpty(plan.index.termType(), plan.index.identifier()))
            {
                columns.put(plan.index, OpenColumnIndex.empty());
                return;
            }

            PerColumnIndexFiles indexFiles = new PerColumnIndexFiles(indexDescriptor,
                                                                     plan.index.termType(),
                                                                     plan.index.identifier());
            OpenColumnIndex column = new OpenColumnIndex(indexFiles);
            try
            {
                MetadataSource metadataSource = MetadataSource.loadColumnMetadata(indexDescriptor, plan.index.identifier());
                List<SegmentMetadata> segments = SegmentMetadata.load(metadataSource, indexDescriptor.primaryKeyFactory);
                for (SegmentMetadata segment : segments)
                {
                    column.segments.add(new OpenSegment(segment,
                                                        IndexSegmentSearcher.open(primaryKeyMapFactory,
                                                                                  descriptor.id,
                                                                                  indexFiles,
                                                                                  segment,
                                                                                  plan.index)));
                }
                columns.put(plan.index, column);
            }
            catch (Throwable throwable)
            {
                column.close();
                if (throwable instanceof IOException)
                {
                    throw (IOException) throwable;
                }
                throw new IOException("Unable to open SAI column " + plan.index.identifier(), throwable);
            }
        }

        @NotNull
        KeyRangeIterator search(@NotNull IndexPlan plan, @NotNull QueryContext queryContext) throws IOException
        {
            OpenColumnIndex column = columns.get(plan.index);
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
                    segmentMatches.add(segment.searcher.search(plan.expression, keyRange, queryContext));
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
                throw new IOException("Unable to search SAI column " + plan.index.identifier(), throwable);
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
            deleteRecursively(temporaryDirectory);
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

    /** Converts the native SAI row iterator to unique partition-key batches without materializing all matches. */
    private static final class CandidatePartitionIterator implements PartitionKeyBatchIterator
    {
        private final KeyRangeIterator matches;
        private final ResourceGroup resources;
        @Nullable
        private final SparkRangeFilter sparkRangeFilter;
        private final int batchSize;
        @Nullable
        private DecoratedKey lastPartitionKey;
        private boolean closed;

        private CandidatePartitionIterator(KeyRangeIterator matches,
                                           ResourceGroup resources,
                                           @Nullable SparkRangeFilter sparkRangeFilter,
                                           int batchSize)
        {
            this.matches = matches;
            this.resources = resources;
            this.sparkRangeFilter = sparkRangeFilter;
            this.batchSize = batchSize;
        }

        @Override
        @NotNull
        public List<PartitionKeyFilter> nextBatch() throws IOException
        {
            if (closed)
            {
                return Collections.emptyList();
            }

            List<PartitionKeyFilter> batch = new ArrayList<>(batchSize);
            try
            {
                while (batch.size() < batchSize && matches.hasNext())
                {
                    PrimaryKey primaryKey = matches.next();
                    DecoratedKey partitionKey = primaryKey.partitionKey();
                    if (lastPartitionKey != null && lastPartitionKey.equals(partitionKey))
                    {
                        continue;
                    }
                    lastPartitionKey = partitionKey;

                    BigInteger token = TokenUtils.tokenToBigInteger(partitionKey.getToken());
                    if (sparkRangeFilter == null || !sparkRangeFilter.skipPartition(token))
                    {
                        batch.add(PartitionKeyFilter.create(partitionKey.getKey().duplicate(), token));
                    }
                }

                // If the native SAI iterator is exhausted, release all index/search resources immediately.
                // The returned Data.db batch is independent of those resources.
                if (!matches.hasNext())
                {
                    close();
                }
                return batch;
            }
            catch (RuntimeException exception)
            {
                close();
                throw exception;
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
            closeQuietly(matches);
            resources.close();
        }
    }

    private static final class EmptyPartitionKeyBatchIterator implements PartitionKeyBatchIterator
    {
        @Override
        public List<PartitionKeyFilter> nextBatch()
        {
            return Collections.emptyList();
        }

        @Override
        public void close()
        {
        }
    }

    private static void materializeIndexComponents(@NotNull SSTable sstable,
                                                   @NotNull IndexDescriptor indexDescriptor,
                                                   @NotNull List<IndexPlan> plans) throws IOException
    {
        for (IndexComponent component : indexDescriptor.version.onDiskFormat()
                                                               .perSSTableIndexComponents(indexDescriptor.hasClustering()))
        {
            copyIfPresent(sstable, indexDescriptor.fileFor(component));
        }
        for (IndexPlan plan : plans)
        {
            for (IndexComponent component : indexDescriptor.version.onDiskFormat().perColumnIndexComponents(plan.index.termType()))
            {
                copyIfPresent(sstable, indexDescriptor.fileFor(component, plan.index.identifier()));
            }
        }
    }

    private static void copyIfPresent(@NotNull SSTable sstable,
                                      @NotNull org.apache.cassandra.io.util.File destination) throws IOException
    {
        try (InputStream input = sstable.openCustomComponent(destination.name()))
        {
            if (input != null)
            {
                Files.copy(input, destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
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

    private static void deleteRecursively(@NotNull Path path)
    {
        try (Stream<Path> paths = Files.walk(path))
        {
            paths.sorted(Comparator.reverseOrder()).forEach(current -> {
                try
                {
                    Files.deleteIfExists(current);
                }
                catch (IOException exception)
                {
                    LOGGER.debug("Unable to delete temporary SAI file {}", current, exception);
                }
            });
        }
        catch (IOException exception)
        {
            LOGGER.debug("Unable to clean temporary SAI directory {}", path, exception);
        }
    }
}
