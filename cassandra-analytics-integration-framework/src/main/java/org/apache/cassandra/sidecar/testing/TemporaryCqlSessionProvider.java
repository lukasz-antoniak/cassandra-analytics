/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.testing;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.internal.core.connection.ExponentialReconnectionPolicy;
import org.apache.cassandra.sidecar.cluster.driver.MultiplexingNodeStateListener;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.jetbrains.annotations.Nullable;

/**
 * A CQL Session provider that always connects to and queries all hosts provided to it.
 * Useful for integration testing, but will eventually be removed once issues with the Sidecar's
 * CQLSessionProviderImpl are resolved.
 */
public class TemporaryCqlSessionProvider implements CQLSessionProvider
{
    private static final Logger logger = LoggerFactory.getLogger(TemporaryCqlSessionProvider.class);
    private final List<InetSocketAddress> contactPoints;
    private CqlSession localSession;
    private final MultiplexingNodeStateListener multiplexingNodeStateListener;

    public TemporaryCqlSessionProvider(List<InetSocketAddress> contactPoints)
    {
        this.contactPoints = contactPoints;
        this.multiplexingNodeStateListener = new MultiplexingNodeStateListener();
    }

    @Nullable
    @Override
    public synchronized CqlSession get()
    {
        try
        {
            if (localSession == null)
            {
                logger.info("Connecting to {}", contactPoints);
                DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                                                                    .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
                                                                    .withClass(DefaultDriverOption.RECONNECTION_POLICY_CLASS, ExponentialReconnectionPolicy.class)
                                                                    .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(100))
                                                                    .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofMillis(1000))
                                                                    .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList())
                                                                    .build();
                CqlSessionBuilder builder = CqlSession.builder()
                                                      .addContactPoints(contactPoints)
                                                      .withNodeStateListener(multiplexingNodeStateListener)
                                                      .withConfigLoader(configLoader);
                localSession = builder.build();
                logger.info("Successfully connected to Cassandra instance!");
            }
        }
        catch (RuntimeException e)
        {
            logger.error("Failed to reach Cassandra", e);
            throw e;
        }
        return localSession;
    }

    @Override
    public CqlSession getIfConnected()
    {
        return this.localSession;
    }

    @Override
    public void registerNodeStateListener(NodeStateListener nodeStateListener)
    {
        multiplexingNodeStateListener.register(nodeStateListener);
    }

    @Override
    public void unregisterNodeStateListener(NodeStateListener nodeStateListener)
    {
        multiplexingNodeStateListener.unregister(nodeStateListener);
    }

    @Override
    public void close()
    {
        CqlSession localSession;
        synchronized (this)
        {
            localSession = this.localSession;
            this.localSession = null;
        }

        if (localSession != null)
        {
            try
            {
                localSession.closeAsync().toCompletableFuture().get(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            catch (TimeoutException e)
            {
                logger.warn("Unable to close session after 1 minute for provider {}", this, e);
            }
            catch (ExecutionException e)
            {
                throw propagateCause(e);
            }
        }
    }

    static RuntimeException propagateCause(ExecutionException e)
    {
        Throwable cause = e.getCause();
        if (cause instanceof Error)
        {
            throw (Error) cause;
        }
        else if (cause instanceof DriverException)
        {
            throw ((DriverException) cause).copy();
        }
        else
        {
            throw new DriverExecutionException(cause);
        }
    }
}
