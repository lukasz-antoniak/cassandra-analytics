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

import org.apache.cassandra.cql3.statements.schema.AlterTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.tcm.ClusterMetadata;

public class SchemaUpdater
{
    private SchemaUpdater()
    {
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata)
    {
        schema.submit(SchemaTransformations.addKeyspace(keyspaceMetadata, false));
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        schema.submit(SchemaTransformations.addTable(tableMetadata, false));
    }

    public static void load(Schema schema, KeyspaceMetadata keyspaceMetadata, Types userTypes)
    {
        schema.submit(SchemaTransformations.addTypes(userTypes, true));
    }

    public static void updateTable(Schema schema, KeyspaceMetadata keyspaceMetadata, TableMetadata tableMetadata)
    {
        schema.submit(new SchemaTransformation()
        {
            public Keyspaces apply(ClusterMetadata clusterMetadata)
            {
                KeyspaceMetadata km = keyspaceMetadata.withSwapped(keyspaceMetadata.tables.withSwapped(tableMetadata));

                // TODO(lantoniak): Remove?
//                KeyspaceMetadata km = clusterMetadata.schema.getKeyspaceMetadata(keyspaceMetadata.name);
//                km = km.withSwapped(km.tables.withSwapped(tableMetadata));

                return clusterMetadata.schema.getKeyspaces().withAddedOrUpdated(km);
            }

            public boolean compatibleWith(ClusterMetadata clusterMetadata)
            {
                return true;
            }
        });
    }
}
