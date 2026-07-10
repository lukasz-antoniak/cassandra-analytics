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

package org.apache.cassandra.bridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerStandard;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.security.EncryptionContext;

public class CassandraTypesImplementation extends AbstractCassandraTypes
{
    public static final CassandraTypesImplementation INSTANCE = new CassandraTypesImplementation();

    public static synchronized void setup(BridgeInitializationParameters params)
    {
        if (!CassandraTypesImplementation.setup)
        {
            // We never want to enable mbean registration in the Cassandra code we use so disable it here
            System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
            System.setProperty("cassandra.schema.force_load_local_keyspaces", "true");
            // Cassandra Analytics requires static initialization of sstable format
            // (it is not being looked up from Sidecar).
            System.setProperty("cassandra.analytics.bridges.sstable_format", "bti");
            Config.setClientMode(true);
            // When we create a TableStreamScanner, we will set the partitioner directly on the table metadata
            // using the supplied IIndexStreamScanner.Partitioner. CFMetaData::compile requires a partitioner to
            // be set in DatabaseDescriptor before we can do that though, so we set one here in preparation.
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
            DatabaseDescriptor.clientInitialization();
            Config config = DatabaseDescriptor.getRawConfig();
            config.memtable_flush_writers = 8;
            config.memtable_heap_space_in_mb = 1024;
            config.memtable_offheap_space_in_mb = 1024;
            config.memtable_cleanup_threshold = 0.5f;
            config.diagnostic_events_enabled = false;
            config.max_mutation_size_in_kb = config.commitlog_segment_size_in_mb * 1024 / 2;
            config.concurrent_compactors = 4;
            config.concurrent_validations = 4;
            config.auto_snapshot = false;
            config.num_tokens = 0;
            Path tempDirectory;
            try
            {
                tempDirectory = Files.createTempDirectory(UUID.randomUUID().toString());
            }
            catch (IOException exception)
            {
                throw new RuntimeException(exception);
            }
            config.data_file_directories = new String[]{tempDirectory.toString()};
            config.metadata_directory = tempDirectory.toString();
            config.default_compaction = new ParameterizedClass("org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy");
            setupCommitLogConfigs(tempDirectory);
            DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
            org.apache.cassandra.io.util.File cassFile = new org.apache.cassandra.io.util.File(tempDirectory.toFile());
            DatabaseDescriptor.setDataDirectories(new org.apache.cassandra.io.util.File[] {cassFile});
            Keyspace.setInitialized();

            setup = true;
        }
    }

    protected static void setupCommitLogConfigs(Path path)
    {
        Path commitLogPath = path.resolve("commitlog");
        DatabaseDescriptor.getRawConfig().commitlog_directory = commitLogPath.toString();
        DatabaseDescriptor.getRawConfig().hints_directory = path.resolve("hints").toString();
        DatabaseDescriptor.getRawConfig().saved_caches_directory = path.resolve("saved_caches").toString();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.periodic);
        DatabaseDescriptor.setEncryptionContext(new EncryptionContext());
        DatabaseDescriptor.setCommitLogSyncPeriod(30);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        DatabaseDescriptor.setCommitLogSyncGroupWindow(30);
        DatabaseDescriptor.setCommitLogSegmentSize(32);
        DatabaseDescriptor.getRawConfig().commitlog_total_space_in_mb = 1024;
        DatabaseDescriptor.setCommitLogSegmentMgrProvider(commitLog -> new CommitLogSegmentManagerStandard(commitLog, new File(commitLogPath)));
    }
}
