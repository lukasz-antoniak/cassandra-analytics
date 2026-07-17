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

package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.ReadOnlyInputStreamFileChannel;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.IndexConsumer;
import org.apache.cassandra.spark.reader.IndexEntry;
import org.apache.cassandra.spark.reader.ReaderUtils;
import org.apache.cassandra.spark.reader.SSTableCache;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.utils.streaming.BufferingInputStream;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.TokenUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.spark.reader.BigIndexReader.calculateCompressedSize;

public class BtiReaderUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BtiReaderUtils.class);

    private static final Set<Component> indexComponents = ImmutableSet.of(Component.DATA,
                                                                          Component.PARTITION_INDEX,
                                                                          Component.ROW_INDEX);

    private BtiReaderUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static TokenRange partitionIndexTokenRange(@NotNull SSTable ssTable,
                                                      @NotNull TableMetadata tableMetadata,
                                                      @NotNull Descriptor descriptor) throws IOException
    {
        AtomicReference<DecoratedKey> firstKey = new AtomicReference<>();
        AtomicReference<DecoratedKey> lastKey = new AtomicReference<>();
        withPartitionIndex(ssTable, descriptor, tableMetadata, false, false, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
            firstKey.set(partitionIndex.firstKey());
            lastKey.set(partitionIndex.lastKey());
        });
        return TokenRange.closed(ReaderUtils.tokenToBigInteger(firstKey.get().getToken()),
                                 ReaderUtils.tokenToBigInteger(lastKey.get().getToken()));
    }

    public static boolean primaryIndexContainsAnyKey(@NotNull SSTable ssTable,
                                                     @NotNull TableMetadata metadata,
                                                     @NotNull Descriptor descriptor,
                                                     @NotNull List<PartitionKeyFilter> filters) throws IOException
    {
        final AtomicBoolean exists = new AtomicBoolean(false);
        withPartitionIndex(ssTable, descriptor, metadata, true, true, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
            TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
            SerializationHeader header = SerializationHeader.make(metadata, Collections.emptyList());
            StatsMetadata sstableMetadata = getStatsMetadata(metadata, header);
            TrieIndexSSTableReader btiTableReader = TrieIndexSSTableReader.internalOpen(descriptor,
                                                                                        indexComponents,
                                                                                        metadataRef,
                                                                                        partitionFileHandle,
                                                                                        dataFileHandle,
                                                                                        partitionIndex,
                                                                                        FilterFactory.AlwaysPresent,
                                                                                        System.currentTimeMillis(),
                                                                                        sstableMetadata,
                                                                                        SSTableReader.OpenReason.NORMAL,
                                                                                        header);

            try (PartitionIterator iter = btiTableReader.allKeysIterator())
            {
                while (!iter.isExhausted())
                {
                    ByteBuffer buffer = iter.key();
                    boolean anyMatch = filters.stream().anyMatch(filter -> filter.matches(buffer));
                    if (anyMatch)
                    {
                        exists.set(true);
                        return;
                    }
                    iter.advance();
                }
            }
            finally
            {
                btiTableReader.selfRef().release();
            }
            exists.set(false);
        });
        return exists.get();
    }

    @Nullable
    public static Long startOffsetInDataFile(@NotNull SSTable ssTable,
                                             @NotNull TableMetadata metadata,
                                             @NotNull Descriptor descriptor,
                                             @NotNull TokenRange tokenRange)
    {
        final AtomicReference<Long> offset = new AtomicReference<>(null);

        Token tokenStart = TokenUtils.bigIntegerToToken(metadata.partitioner, tokenRange.lowerEndpoint());
        Token tokenEnd = TokenUtils.bigIntegerToToken(metadata.partitioner, tokenRange.upperEndpoint());
        Range<Token> range = new Range<>(tokenStart, tokenEnd);

        try
        {
            withPartitionIndex(ssTable, descriptor, metadata, true, false, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
                TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
                SerializationHeader header = SerializationHeader.make(metadata, Collections.emptyList());
                StatsMetadata sstableMetadata = getStatsMetadata(metadata, header);
                TrieIndexSSTableReader btiTableReader = TrieIndexSSTableReader.internalOpen(descriptor,
                                                                                            indexComponents,
                                                                                            metadataRef,
                                                                                            partitionFileHandle,
                                                                                            dataFileHandle,
                                                                                            partitionIndex,
                                                                                            FilterFactory.AlwaysPresent,
                                                                                            System.currentTimeMillis(),
                                                                                            sstableMetadata,
                                                                                            SSTableReader.OpenReason.NORMAL,
                                                                                            header);
                try
                {
                    List<SSTableReader.PartitionPositionBounds> positions =
                            btiTableReader.getPositionsForRanges(Collections.singletonList(range));
                    if (!positions.isEmpty())
                    {
                        // we should receive zero or one position
                        offset.set(positions.get(0).lowerPosition);
                    }
                }
                finally
                {
                    btiTableReader.selfRef().release();
                }
            });
        }
        catch (IOException e)
        {
            LOGGER.warn("Failed to lookup start offset for token range {} in sstable {}",
                        tokenRange, ssTable, e);
        }
        return offset.get();
    }

    public static void consumePrimaryIndex(@NotNull SSTable ssTable,
                                           @NotNull TableMetadata metadata,
                                           @NotNull Descriptor descriptor,
                                           @Nullable SparkRangeFilter range,
                                           @NotNull IndexConsumer consumer) throws IOException
    {
        long dataFileLength = ssTable.length(FileType.DATA);
        TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);
        org.apache.cassandra.spark.reader.CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetadata(
        ssTable, descriptor.version.hasMaxCompressedLength(), metadata.params.crcCheckChance);

        withPartitionIndex(ssTable, descriptor, metadata, true, true, (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
            SerializationHeader header = SerializationHeader.make(metadata, Collections.emptyList());
            StatsMetadata sstableMetadata = getStatsMetadata(metadata, header);
            TrieIndexSSTableReader btiTableReader = TrieIndexSSTableReader.internalOpen(descriptor,
                                                                                        indexComponents,
                                                                                        metadataRef,
                                                                                        partitionFileHandle,
                                                                                        dataFileHandle,
                                                                                        partitionIndex,
                                                                                        FilterFactory.AlwaysPresent,
                                                                                        System.currentTimeMillis(),
                                                                                        sstableMetadata,
                                                                                        SSTableReader.OpenReason.NORMAL,
                                                                                        header);
            try (PartitionIterator iter = btiTableReader.allKeysIterator())
            {
                ByteBuffer prevKey = null;
                long prevPos = 0;
                BigInteger prevToken = null;
                boolean started = false;
                while (!iter.isExhausted())
                {
                    ByteBuffer key = iter.key();
                    long pos = iter.dataPosition();
                    DecoratedKey decoratedKey = metadata.partitioner.decorateKey(key);
                    BigInteger token = ReaderUtils.tokenToBigInteger(decoratedKey.getToken());

                    // TODO: Implement reporting statistics.
                    if (prevKey != null && (range == null || range.overlaps(prevToken)))
                    {
                        // we reached the end of the file, so consume last key if overlaps
                        started = true;
                        long uncompressed = pos - prevPos;
                        long compressed = compressionMetadata == null
                                          ? uncompressed
                                          : calculateCompressedSize(compressionMetadata, dataFileLength, prevPos, pos - 1);
                        consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
                    }
                    else if (started)
                    {
                        // we have gone passed the range we care about so exit early
                        return;
                    }

                    prevKey = key;
                    prevPos = pos;
                    prevToken = token;

                    iter.advance();
                }

                if (prevKey != null && (range == null || range.overlaps(prevToken)))
                {
                    // we reached the end of the file, so consume last key if overlaps
                    long end = (compressionMetadata == null ? dataFileLength : compressionMetadata.getDataLength());
                    long uncompressed = end - prevPos;
                    long compressed = compressionMetadata == null
                                      ? uncompressed
                                      : calculateCompressedSize(compressionMetadata, dataFileLength, prevPos, end - 1);
                    consumer.accept(new IndexEntry(prevKey, prevToken, uncompressed, compressed));
                }
            }
            finally
            {
                btiTableReader.selfRef().release();
            }
        });
    }

    public static void readPrimaryIndex(@NotNull SSTable ssTable,
                                        @NotNull IPartitioner partitioner,
                                        @NotNull Descriptor descriptor,
                                        double crcCheckChance,
                                        @NotNull Function<ByteBuffer, Boolean> tracker) throws IOException
    {
        withPartitionIndex(ssTable, descriptor, partitioner, crcCheckChance, true, true,
                           (dataFileHandle, partitionFileHandle, rowFileHandle, partitionIndex) -> {
                               try (PartitionIterator iter = new PartitionIterator(partitionIndex, partitioner,
                                                                                   rowFileHandle, dataFileHandle))
                               {
                                   while (!iter.isExhausted())
                                   {
                                       ByteBuffer key = iter.key();
                                       if (tracker.apply(key))
                                       {
                                           // exit early if tracker returns true
                                           return;
                                       }
                                       iter.advance();
                                   }
                               }
                           });
    }

    private static void withPartitionIndex(@NotNull SSTable ssTable,
                                           @NotNull Descriptor descriptor,
                                           @NotNull TableMetadata metadata,
                                           boolean loadDataFile,
                                           boolean loadRowsIndex,
                                           @NotNull BtiPartitionIndexConsumer consumer) throws IOException
    {
        withPartitionIndex(ssTable, descriptor, metadata.partitioner, metadata.params.crcCheckChance, loadDataFile, loadRowsIndex, consumer);
    }

    private static void withPartitionIndex(@NotNull SSTable ssTable,
                                           @NotNull Descriptor descriptor,
                                           @NotNull IPartitioner partitioner,
                                           double crcCheckChance,
                                           boolean loadDataFile,
                                           boolean loadRowsIndex,
                                           @NotNull BtiPartitionIndexConsumer consumer) throws IOException
    {
        File file = new File(ssTable.getDataFileName());

        try (CompressionMetadata compression = getCompressionMetadata(ssTable, crcCheckChance, descriptor);
             FileHandle dataFileHandle = loadDataFile ? createFileHandle(file,
                                                                         ssTable.openDataStream(),
                                                                         ssTable.length(FileType.DATA),
                                                                         compression).complete() : null;
             FileHandle.Builder partitionFileHandle = createFileHandle(file,
                                                                       ssTable.openPrimaryIndexStream(),
                                                                       ssTable.length(FileType.PARTITIONS_INDEX),
                                                                       null);
             FileHandle rowFileHandle = loadRowsIndex ? createFileHandle(file,
                                                                         ssTable.openRowIndexStream(),
                                                                         ssTable.length(FileType.ROWS_INDEX),
                                                                         null).complete() : null;
             PartitionIndex partitionIndex = PartitionIndex.load(partitionFileHandle, partitioner, false, ByteComparable.Version.OSS41))
        {
            consumer.accept(dataFileHandle, partitionFileHandle.complete(), rowFileHandle, partitionIndex);
        }
    }

    private static FileHandle.Builder createFileHandle(File file, InputStream stream, long size, CompressionMetadata compression) throws IOException
    {
        if (stream == null)
        {
            throw new FileNotFoundException("Cannot find file " + file.absolutePath());
        }
        ReadOnlyInputStreamFileChannel fileChannel = new ReadOnlyInputStreamFileChannel((BufferingInputStream<?>) stream, size);
        ChannelProxy proxy = new ChannelProxy(file, fileChannel);
        FileHandle.Builder builder = new FileHandle.Builder(proxy);
        if (compression != null)
        {
            builder.withCompressionMetadata(compression);
        }
        return builder;
    }

    private static CompressionMetadata getCompressionMetadata(SSTable ssTable,
                                                              double crcCheckChance,
                                                              Descriptor descriptor) throws IOException
    {
        org.apache.cassandra.spark.reader.CompressionMetadata compressionMetadata = SSTableCache.INSTANCE.compressionMetadata(
        ssTable, descriptor.version.hasMaxCompressedLength(), crcCheckChance);
        if (compressionMetadata != null)
        {
            return compressionMetadata.toInternal(descriptor.fileFor(Component.COMPRESSION_INFO),
                                                  ssTable.length(FileType.DATA));
        }
        return null;
    }

    private static StatsMetadata getStatsMetadata(@NotNull TableMetadata metadata, @NotNull SerializationHeader header)
    {
        return (StatsMetadata) new MetadataCollector(metadata.comparator)
                               .finalizeMetadata(metadata.partitioner.getClass().getCanonicalName(),
                                                 metadata.params.bloomFilterFpChance, UNREPAIRED_SSTABLE,
                                                 null, false, header)
                               .get(MetadataType.STATS);
    }

    public interface BtiPartitionIndexConsumer
    {
        void accept(FileHandle dataFile, FileHandle partitionFile, FileHandle rowFile, PartitionIndex partitionIndex) throws IOException;
    }
}
