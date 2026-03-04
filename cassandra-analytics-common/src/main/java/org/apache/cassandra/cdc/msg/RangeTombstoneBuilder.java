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

package org.apache.cassandra.cdc.msg;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Keep track of the last range tombstone marker to build {@link RangeTombstone}
 * The caller should check whether {@link #canBuild()} after adding marker, and it should build whenever possible.
 * IMPLEMENTATION NOTE: Refactored from abstract class to interface, due to classloader clash. Superclass is loaded
 * by application classloader, but concrete implementation (from bridge module) with dedicated classloader.
 *
 * @param <T>
 */
public interface RangeTombstoneBuilder<T>
{
    RangeTombstone buildTombstone(List<Value> start, boolean isStartInclusive, List<Value> end, boolean isEndInclusive);
    boolean canBuild();
    RangeTombstone build();
    void add(T marker);

    /**
     * @return true when there is range tombstone marker not consumed.
     */
    boolean hasIncompleteRange();

    Value buildValue(String keyspace, String name, String type, ByteBuffer buf);
}
