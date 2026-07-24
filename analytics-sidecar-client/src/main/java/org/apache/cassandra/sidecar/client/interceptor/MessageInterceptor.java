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

package org.apache.cassandra.sidecar.client.interceptor;

import java.util.Map;

import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.RequestContext;
import org.jetbrains.annotations.ApiStatus;

/**
 * Intercepts raw HTTP communication between the application and the Sidecar process.
 *
 * Implementations can inspect, mutate, or log requests and responses.
 * Interceptors are registered via the plugin system and executed sequentially
 * in the order of registration (see {@code BulkSparkConf#SIDECAR_MESSAGE_INTERCEPTORS} property).
 *
 * Implementations must be thread-safe as methods may be invoked
 * concurrently from multiple worker threads. Try to implement any IO interaction
 * outside of {@link #onRequest(RequestContext.Builder)}, {@link #onResponse(RequestContext, HttpResponse, Object)}
 * and {@link #onFailure(RequestContext, Throwable)} callbacks.
 */
@ApiStatus.Experimental
public interface MessageInterceptor
{
    /**
     * Initializes message interceptor. Method can be invoked form Spark driver and executors.
     * @param options Spark configuration
     */
    default void initialize(Map<String, String> options)
    {
    }

    /**
     * Callback executed before request is build and send to Sidecar.
     * @param requestBuilder the request builder
     */
    default void onRequest(RequestContext.Builder requestBuilder)
    {
    }

    /**
     * Callback executed after response has been received from Sidecar.
     * @param requestContext request context
     * @param response       HTTP response
     * @param responseObject Deserialized response object
     */
    default void onResponse(RequestContext requestContext, HttpResponse response, Object responseObject)
    {
    }

    /**
     * Callback executed on error when calling Sidecar.
     * @param requestContext request context
     * @param error          exception encountered
     */
    default void onFailure(RequestContext requestContext, Throwable error)
    {
    }
}
