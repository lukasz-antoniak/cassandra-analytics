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

package org.apache.cassandra.sidecar.client;

import java.util.Map;

import org.jetbrains.annotations.ApiStatus;

/**
 * Extension point for custom identity provider for Sidecar communication.
 *
 * Identity provider is registered by specifying fully qualified class name
 * in {@code BulkSparkConf#SIDECAR_IDENTITY_PROVIDER} property. Implementations
 * have to provide no-arguments constructor. Initialization code shall be put in
 * {@link #initialize(Map, HttpClient)} method. Implementations must be thread-safe as methods
 * may be invoked concurrently from multiple worker threads. Try to invoke any
 * long running IO interaction outside of {@link #injectCredentials(RequestContext.Builder)} callback.
 */
@ApiStatus.Experimental
public interface SidecarIdentityProvider
{
    SidecarIdentityProvider NOOP = requestBuilder -> {};

    /**
     * Initializes identity provider. Method can be invoked form Spark driver and executors.
     * @param options    Identity provider's options passed as part of Spark configuration (prefixed by
     *                   {@code BulkSparkConf#SIDECAR_IDENTITY_PROVIDER_PARAMETER_PREFIX}). For example,
     *                   {@code "spark.cassandra_analytics.sidecar.identity.provider.parameter.param1" = "value1"}
     *                   will result in map {@code "param1" = "value1"}.
     * @param httpClient HTTP client
     */
    default void initialize(Map<String, String> options, HttpClient httpClient)
    {
    }

    /**
     * Callback executed before request is build and send to Sidecar. Typical identity
     * provider will add custom HTTP headers to the request.
     * @param requestBuilder the request builder
     */
    void injectCredentials(RequestContext.Builder requestBuilder);
}
