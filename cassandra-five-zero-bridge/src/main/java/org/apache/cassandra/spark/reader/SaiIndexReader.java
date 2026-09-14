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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
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
 * Reads Cassandra 5 SAI components directly from SSTables and exposes matching partition keys as bounded batches.
 *
 * <p>SAI is deliberately only a pruning hint. Every candidate partition is still read from all participating
 * SSTables and passed through the normal compaction/reconciliation path before rows are returned. This is required
 * because a newer SSTable can contain an update or tombstone which is not a hit in its own SAI index.</p>
 *
 * <p>To avoid keeping every SAI index open for the lifetime of a Spark task, each SSTable owns a paged candidate
 * source. A source materializes its SAI components once, but opens SAI readers only while filling a small in-memory
 * page and closes them immediately afterwards. The globally sorted candidate stream is merged from those pages.
 * Consequently, SAI reader/file-handle usage is bounded to one SSTable refill at a time, while candidate memory is
 * bounded by the per-SSTable prefetch size plus the reconciliation batch.</p>
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
     * Opens a streaming SAI candidate iterator over all supplied SSTables.
     *
     * <p>Each SSTable is preflighted sequentially and only its first bounded page is retained in memory. This ensures
     * missing/corrupt SAI components are detected before any rows can be emitted, so the caller can safely fall back
     * to a normal SSTable scan. Later page failures are surfaced to the caller; falling back after rows have already
     * been emitted could otherwise duplicate results.</p>
     *
     * @return {@link Optional#empty()} when SAI cannot safely be opened and the caller must fall back to a normal
     *         scan; otherwise a closeable iterator that yields bounded, globally ordered candidate batches
     */
    @NotNull
    public static Optional<CandidatePartitionIterator> openCandidatePartitionIterator(@NotNull TableMetadata metadata,
                                                                                       @NotNull Set<SSTable> sstables,
                                                                                       @NotNull List<SaiFilter> filters,
                                                                                       @Nullable SparkRangeFilter sparkRangeFilter,
                                                                                       int prefetchSize,
                                                                                       int batchSize)
    {
        validateSize("SAI candidate prefetch size", prefetchSize);
        validateSize("SAI candidate batch size", batchSize);

        if (sstables.isEmpty() || filters.isEmpty())
        {
            return Optional.of(CandidatePartitionIterator.empty(batchSize));
        }

        // Use one predicate as the physical pruning hint. Spark still evaluates every predicate after the scan,
        // and using a single predicate avoids duplicating SAI's expression-normalization/planning logic here.
        List<SaiFilter> selectedFilters = Collections.singletonList(filters.get(0));
        List<CandidateSource> sources = new ArrayList<>(sstables.size());
        try
        {
            StorageAttachedIndex index = storageAttachedIndex(metadata, selectedFilters.get(0).index());
            if (!canSearch(index, selectedFilters))
            {
                return Optional.empty();
            }

            Expression expression = Expression.create(index);
            for (SaiFilter filter : selectedFilters)
            {
                expression.add(operator(filter.operator()), index.termType().fromString(filter.value()));
            }

            for (SSTable sstable : sstables)
            {
                sources.add(new PagedSSTableCandidateSource(metadata,
                                                            sstable,
                                                            index,
                                                            expression,
                                                            sparkRangeFilter,
                                                            prefetchSize));
            }

            // CandidatePartitionIterator.open() advances every source once. That performs all SAI preflight and
            // first-page reads sequentially, so no two SSTables need open SAI search resources at the same time.
            return Optional.of(CandidatePartitionIterator.open(sources, batchSize));
        }
        catch (Throwable throwable)
        {
            closeSources(sources);
            // SAI is an optimization only. Unsupported/corrupt/missing components detected before any rows are
            // emitted fail open to the existing SSTable scan so query correctness is unchanged.
            LOGGER.warn("Unable to use SAI for SSTable pruning; falling back to normal SSTable scan", throwable);
            return Optional.empty();
        }
    }

    @VisibleForTesting
    @NotNull
    static CandidatePartitionIterator candidatePartitionIterator(@NotNull List<CandidateSource> sources,
                                                                  int batchSize) throws IOException
    {
        validateSize("SAI candidate batch size", batchSize);
        return CandidatePartitionIterator.open(sources, batchSize);
    }

    private static void validateSize(@NotNull String name, int value)
    {
        if (value <= 0)
        {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    /**
     * A sorted source of candidate partitions. Implementations keep at most one current candidate positioned.
     */
    @VisibleForTesting
    interface CandidateSource extends Closeable
    {
        /**
         * Advances to the next candidate partition.
         *
         * @return true when {@link #current()} is positioned, false when the source is exhausted
         */
        boolean advance() throws IOException;

        @NotNull
        PartitionKeyFilter current();
    }

    /**
     * Per-SSTable candidate source. SAI readers are opened only inside {@link #loadNextPage()} and closed before the
     * method returns; only the materialized component files and a bounded page of partition keys survive between
     * calls.
     */
    private static final class PagedSSTableCandidateSource implements CandidateSource
    {
        @NotNull
        private final TableMetadata metadata;
        @NotNull
        private final SSTable sstable;
        @NotNull
        private final StorageAttachedIndex index;
        @NotNull
        private final Expression expression;
        @Nullable
        private final SparkRangeFilter sparkRangeFilter;
        private final int prefetchSize;
        @NotNull
        private final Deque<PartitionKeyFilter> buffered = new ArrayDeque<>();

        @Nullable
        private Path temporaryDirectory;
        @Nullable
        private Descriptor descriptor;
        @Nullable
        private IndexDescriptor indexDescriptor;
        @NotNull
        private List<SegmentMetadata> segments = Collections.emptyList();
        @Nullable
        private DecoratedKey resumeAfter;
        @Nullable
        private PartitionKeyFilter current;
        private boolean initialized;
        private boolean exhausted;
        private boolean closed;

        private PagedSSTableCandidateSource(@NotNull TableMetadata metadata,
                                            @NotNull SSTable sstable,
                                            @NotNull StorageAttachedIndex index,
                                            @NotNull Expression expression,
                                            @Nullable SparkRangeFilter sparkRangeFilter,
                                            int prefetchSize)
        {
            this.metadata = metadata;
            this.sstable = sstable;
            this.index = index;
            this.expression = expression;
            this.sparkRangeFilter = sparkRangeFilter;
            this.prefetchSize = prefetchSize;
        }

        @Override
        public boolean advance() throws IOException
        {
            if (closed)
            {
                return false;
            }

            current = null;
            while (buffered.isEmpty() && !exhausted)
            {
                loadNextPage();
            }

            if (buffered.isEmpty())
            {
                close();
                return false;
            }

            current = buffered.removeFirst();
            return true;
        }

        @Override
        @NotNull
        public PartitionKeyFilter current()
        {
            if (current == null)
            {
                throw new IllegalStateException("SAI candidate source is not positioned");
            }
            return current;
        }

        private void initialize() throws IOException
        {
            if (initialized)
            {
                return;
            }
            initialized = true;

            if (sstable.customComponentNames().isEmpty())
            {
                throw new IOException("SSTable has no SAI custom components: " + sstable.getDataFileName());
            }

            temporaryDirectory = Files.createTempDirectory("cassandra-analytics-sai-");
            descriptor = descriptor(metadata, sstable, temporaryDirectory);
            indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator);
            materializeIndexComponents(sstable, indexDescriptor, index);

            if (!indexDescriptor.isPerSSTableIndexBuildComplete()
                || !indexDescriptor.isPerColumnIndexBuildComplete(index.identifier()))
            {
                throw new IOException("SAI index components are incomplete for SSTable " + sstable.getDataFileName());
            }

            if (indexDescriptor.isIndexEmpty(index.termType(), index.identifier()))
            {
                exhausted = true;
                return;
            }

            MetadataSource metadataSource = MetadataSource.loadColumnMetadata(indexDescriptor, index.identifier());
            segments = SegmentMetadata.load(metadataSource, indexDescriptor.primaryKeyFactory);
            exhausted = segments.isEmpty();
        }

        private void loadNextPage() throws IOException
        {
            initialize();
            if (exhausted)
            {
                return;
            }

            // All resources below are local to a single refill. No SAI searcher, PrimaryKeyMap, or index-file reader
            // survives after this method returns, so refilling one source never holds another SSTable's SAI open.
            List<IndexSegmentSearcher> searchers = new ArrayList<>();
            List<KeyRangeIterator> segmentMatches = new ArrayList<>();
            KeyRangeIterator matches = null;
            try (PrimaryKeyMap.Factory primaryKeyMapFactory = indexDescriptor.newPrimaryKeyMapFactory(null);
                 PerColumnIndexFiles indexFiles = new PerColumnIndexFiles(indexDescriptor,
                                                                          index.termType(),
                                                                          index.identifier()))
            {
                try
                {
                    QueryContext queryContext = new QueryContext(null, NO_TIMEOUT_MILLIS);
                    for (SegmentMetadata segment : segments)
                    {
                        AbstractBounds<PartitionPosition> keyRange = remainingRange(segment, resumeAfter);
                        if (keyRange == null)
                        {
                            continue;
                        }

                        IndexSegmentSearcher searcher = IndexSegmentSearcher.open(primaryKeyMapFactory,
                                                                                   descriptor.id,
                                                                                   indexFiles,
                                                                                   segment,
                                                                                   index);
                        searchers.add(searcher);
                        segmentMatches.add(searcher.search(expression, keyRange, queryContext));
                    }

                    if (segmentMatches.isEmpty())
                    {
                        exhausted = true;
                        return;
                    }

                    matches = KeyRangeUnionIterator.build(segmentMatches);
                    // The union iterator owns/closes its child iterators from here on.
                    segmentMatches.clear();

                    DecoratedKey lastSeenInPage = null;
                    boolean pageFull = false;
                    while (matches.hasNext())
                    {
                        PrimaryKey primaryKey = matches.next();
                        DecoratedKey partitionKey = primaryKey.partitionKey();
                        if (lastSeenInPage != null && lastSeenInPage.equals(partitionKey))
                        {
                            continue;
                        }

                        lastSeenInPage = partitionKey;
                        // Resume from an exclusive partition bound. If a wide partition has more matching clustering
                        // rows than fit in this page, reopening the SAI search will skip the entire already-emitted
                        // partition instead of returning it in the next reconciliation batch.
                        resumeAfter = partitionKey;

                        BigInteger token = TokenUtils.tokenToBigInteger(partitionKey.getToken());
                        if (sparkRangeFilter == null || !sparkRangeFilter.skipPartition(token))
                        {
                            buffered.addLast(PartitionKeyFilter.create(partitionKey.getKey().duplicate(), token));
                            if (buffered.size() >= prefetchSize)
                            {
                                pageFull = true;
                                break;
                            }
                        }
                    }

                    // If the page was not filled, every remaining segment iterator was exhausted and there cannot be
                    // a later match for this SSTable. A page that fills exactly at EOF may cause one harmless refill.
                    exhausted = !pageFull;
                }
                finally
                {
                    // Close iterators/searchers before their shared index files and PrimaryKeyMap factory are closed by
                    // the surrounding try-with-resources block.
                    closeQuietly(matches);
                    for (KeyRangeIterator segmentMatch : segmentMatches)
                    {
                        closeQuietly(segmentMatch);
                    }
                    for (IndexSegmentSearcher searcher : searchers)
                    {
                        closeQuietly(searcher);
                    }
                }
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
            buffered.clear();
            current = null;
            if (temporaryDirectory != null)
            {
                deleteRecursively(temporaryDirectory);
                temporaryDirectory = null;
            }
        }
    }

    @Nullable
    private static AbstractBounds<PartitionPosition> remainingRange(@NotNull SegmentMetadata segment,
                                                                     @Nullable DecoratedKey resumeAfter)
    {
        DecoratedKey first = segment.minKey.partitionKey();
        DecoratedKey last = segment.maxKey.partitionKey();
        if (resumeAfter == null || resumeAfter.compareTo(first) < 0)
        {
            return new Bounds<>(first, last);
        }
        if (resumeAfter.compareTo(last) >= 0)
        {
            return null;
        }
        return new Range<>(resumeAfter, last);
    }

    /**
     * Globally merges the sorted per-SSTable candidate sources and emits bounded reconciliation batches.
     */
    public static final class CandidatePartitionIterator implements PartitionKeyBatchIterator
    {
        @NotNull
        private final List<CandidateSource> sources;
        @NotNull
        private final PriorityQueue<CandidateSource> candidates;
        private final int batchSize;
        private boolean closed;

        private CandidatePartitionIterator(@NotNull List<CandidateSource> sources,
                                           @NotNull PriorityQueue<CandidateSource> candidates,
                                           int batchSize)
        {
            this.sources = sources;
            this.candidates = candidates;
            this.batchSize = batchSize;
        }

        @NotNull
        private static CandidatePartitionIterator open(@NotNull List<CandidateSource> sources,
                                                       int batchSize) throws IOException
        {
            List<CandidateSource> ownedSources = new ArrayList<>(sources);
            PriorityQueue<CandidateSource> candidates = new PriorityQueue<>(Comparator.comparing(CandidateSource::current));
            try
            {
                // Sequentially obtain one head candidate from each SSTable. PagedSSTableCandidateSource closes all
                // SAI search handles before advance() returns, so only one SSTable's SAI is open at any instant.
                for (CandidateSource source : ownedSources)
                {
                    if (source.advance())
                    {
                        candidates.add(source);
                    }
                    else
                    {
                        closeQuietly(source);
                    }
                }
                return new CandidatePartitionIterator(ownedSources, candidates, batchSize);
            }
            catch (IOException | RuntimeException exception)
            {
                closeSources(ownedSources);
                throw exception;
            }
        }

        @NotNull
        private static CandidatePartitionIterator empty(int batchSize)
        {
            return new CandidatePartitionIterator(Collections.emptyList(),
                                                  new PriorityQueue<>(Comparator.comparing(CandidateSource::current)),
                                                  batchSize);
        }

        /**
         * Returns the next globally ordered batch of unique matching partition keys, or an empty list when exhausted.
         */
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
                while (batch.size() < batchSize && !candidates.isEmpty())
                {
                    CandidateSource first = candidates.poll();
                    PartitionKeyFilter candidate = first.current();
                    batch.add(candidate);

                    // Consume this candidate from every SSTable before moving on. This globally deduplicates a
                    // partition that appears in multiple SAI indexes without retaining an unbounded seen-key set.
                    advancePast(first, candidate);
                    while (!candidates.isEmpty() && candidates.peek().current().compareTo(candidate) == 0)
                    {
                        advancePast(candidates.poll(), candidate);
                    }
                }

                if (candidates.isEmpty())
                {
                    close();
                }
                return batch;
            }
            catch (IOException | RuntimeException exception)
            {
                close();
                throw exception;
            }
        }

        private void advancePast(@NotNull CandidateSource source,
                                 @NotNull PartitionKeyFilter emitted) throws IOException
        {
            while (source.advance())
            {
                if (source.current().compareTo(emitted) != 0)
                {
                    candidates.add(source);
                    return;
                }
            }
            closeQuietly(source);
        }

        @Override
        public void close()
        {
            if (!closed)
            {
                closed = true;
                candidates.clear();
                closeSources(sources);
            }
        }
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
    private static Descriptor descriptor(@NotNull TableMetadata metadata,
                                         @NotNull SSTable sstable,
                                         @NotNull Path temporaryDirectory)
    {
        org.apache.cassandra.io.util.File dataFile = new org.apache.cassandra.io.util.File(temporaryDirectory.resolve(sstable.getDataFileName()));
        return Descriptor.fromFileWithComponent(dataFile, metadata.keyspace, metadata.name).left;
    }

    private static void materializeIndexComponents(@NotNull SSTable sstable,
                                                   @NotNull IndexDescriptor indexDescriptor,
                                                   @NotNull StorageAttachedIndex index)
    throws IOException
    {
        for (IndexComponent component : indexDescriptor.version.onDiskFormat().perSSTableIndexComponents(indexDescriptor.hasClustering()))
        {
            copyIfPresent(sstable, indexDescriptor.fileFor(component));
        }
        for (IndexComponent component : indexDescriptor.version.onDiskFormat().perColumnIndexComponents(index.termType()))
        {
            copyIfPresent(sstable, indexDescriptor.fileFor(component, index.identifier()));
        }
    }

    private static void copyIfPresent(@NotNull SSTable sstable, @NotNull org.apache.cassandra.io.util.File destination)
    throws IOException
    {
        try (InputStream input = sstable.openCustomComponent(destination.name()))
        {
            if (input != null)
            {
                Files.copy(input, destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
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

    private static void closeSources(@NotNull List<? extends CandidateSource> sources)
    {
        for (CandidateSource source : sources)
        {
            closeQuietly(source);
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
        catch (Throwable throwable)
        {
            LOGGER.debug("Unable to close SAI search resource", throwable);
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
