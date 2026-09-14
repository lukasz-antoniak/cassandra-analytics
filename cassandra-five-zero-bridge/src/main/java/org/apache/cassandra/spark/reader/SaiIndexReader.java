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
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
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
 * Reads Cassandra 5 SAI components directly from an SSTable and returns candidate partition keys.
 *
 * <p>The result is deliberately only a pruning hint. The caller must read each candidate partition from all
 * participating SSTables and use the normal compaction/reconciliation path before returning rows. This is required
 * because SAI does not represent all deletion/update state needed to reconcile multiple SSTables.</p>
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
     * Finds candidate partition keys for one SAI index across the supplied SSTables.
     *
     * @return {@link Optional#empty()} when SAI cannot safely be used and the caller must fall back to a normal scan;
     *         otherwise the complete candidate set (which may be empty)
     */
    @NotNull
    public static Optional<Set<PartitionKeyFilter>> findCandidatePartitionKeys(@NotNull TableMetadata metadata,
                                                                               @NotNull Set<SSTable> sstables,
                                                                               @NotNull List<SaiFilter> filters,
                                                                               @Nullable SparkRangeFilter sparkRangeFilter,
                                                                               int maxCandidatePartitions)
    {
        if (sstables.isEmpty() || filters.isEmpty())
        {
            return Optional.of(new HashSet<>());
        }

        // Use one predicate as the physical pruning hint. Spark still evaluates every predicate after the scan,
        // and using a single predicate avoids having to duplicate SAI's expression-normalization/planning logic here.
        List<SaiFilter> selectedFilters = new ArrayList<>(1);
        selectedFilters.add(filters.get(0));

        try
        {
            StorageAttachedIndex index = storageAttachedIndex(metadata, selectedFilters.get(0).index());
            if (!canSearch(index, selectedFilters))
            {
                return Optional.empty();
            }

            Set<PartitionKeyFilter> candidates = new HashSet<>();
            for (SSTable sstable : sstables)
            {
                Optional<Set<PartitionKeyFilter>> sstableCandidates = findCandidatePartitionKeys(metadata,
                                                                                                   sstable,
                                                                                                   index,
                                                                                                   selectedFilters,
                                                                                                   sparkRangeFilter);
                if (!sstableCandidates.isPresent())
                {
                    return Optional.empty();
                }

                candidates.addAll(sstableCandidates.get());
                if (candidates.size() > maxCandidatePartitions)
                {
                    LOGGER.debug("SAI candidate count {} exceeded limit {}; falling back to SSTable scan",
                                 candidates.size(), maxCandidatePartitions);
                    return Optional.empty();
                }
            }
            return Optional.of(candidates);
        }
        catch (Throwable throwable)
        {
            // SAI is an optimization only. Any unsupported/corrupt/missing component must fail open to the
            // existing SSTable scan so query correctness is unchanged.
            LOGGER.warn("Unable to use SAI for SSTable pruning; falling back to normal SSTable scan", throwable);
            return Optional.empty();
        }
    }

    private static Optional<Set<PartitionKeyFilter>> findCandidatePartitionKeys(@NotNull TableMetadata metadata,
                                                                                 @NotNull SSTable sstable,
                                                                                 @NotNull StorageAttachedIndex index,
                                                                                 @NotNull List<SaiFilter> filters,
                                                                                 @Nullable SparkRangeFilter sparkRangeFilter)
    throws IOException
    {
        if (sstable.customComponentNames().isEmpty())
        {
            return Optional.empty();
        }

        Path temporaryDirectory = Files.createTempDirectory("cassandra-analytics-sai-");
        try
        {
            Descriptor descriptor = descriptor(metadata, sstable, temporaryDirectory);
            IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator);
            materializeIndexComponents(sstable, indexDescriptor, index);

            if (!indexDescriptor.isPerSSTableIndexBuildComplete()
                || !indexDescriptor.isPerColumnIndexBuildComplete(index.identifier()))
            {
                return Optional.empty();
            }

            if (indexDescriptor.isIndexEmpty(index.termType(), index.identifier()))
            {
                return Optional.of(new HashSet<>());
            }

            Expression expression = Expression.create(index);
            for (SaiFilter filter : filters)
            {
                expression.add(operator(filter.operator()), index.termType().fromString(filter.value()));
            }

            Set<PartitionKeyFilter> candidates = new HashSet<>();
            MetadataSource metadataSource = MetadataSource.loadColumnMetadata(indexDescriptor, index.identifier());
            List<SegmentMetadata> segments = SegmentMetadata.load(metadataSource, indexDescriptor.primaryKeyFactory);
            QueryContext queryContext = new QueryContext(null, NO_TIMEOUT_MILLIS);

            try (PrimaryKeyMap.Factory primaryKeyMapFactory = indexDescriptor.newPrimaryKeyMapFactory(null);
                 PerColumnIndexFiles indexFiles = new PerColumnIndexFiles(indexDescriptor, index.termType(), index.identifier()))
            {
                for (SegmentMetadata segment : segments)
                {
                    Bounds<PartitionPosition> keyRange = new Bounds<>(segment.minKey.partitionKey(), segment.maxKey.partitionKey());
                    try (IndexSegmentSearcher searcher = IndexSegmentSearcher.open(primaryKeyMapFactory,
                                                                                    descriptor.id,
                                                                                    indexFiles,
                                                                                    segment,
                                                                                    index);
                         KeyRangeIterator matches = searcher.search(expression, keyRange, queryContext))
                    {
                        while (matches.hasNext())
                        {
                            PrimaryKey primaryKey = matches.next();
                            DecoratedKey partitionKey = primaryKey.partitionKey();
                            BigInteger token = TokenUtils.tokenToBigInteger(partitionKey.getToken());
                            if (sparkRangeFilter == null || !sparkRangeFilter.skipPartition(token))
                            {
                                candidates.add(PartitionKeyFilter.create(partitionKey.getKey().duplicate(), token));
                            }
                        }
                    }
                }
            }
            return Optional.of(candidates);
        }
        finally
        {
            deleteRecursively(temporaryDirectory);
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
