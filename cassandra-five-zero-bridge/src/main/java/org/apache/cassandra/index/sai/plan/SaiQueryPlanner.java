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
package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SaiIndex;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.apache.cassandra.spark.utils.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Adapts the version-neutral Spark SAI filter tree to Cassandra's native SAI query planner.
 *
 * <p>Cassandra 5.0's SAI {@code Operation} planner accepts a flat conjunctive {@link RowFilter}. Spark can push an
 * OR tree, so the only boolean transformation performed here is a bounded conversion to disjunctive normal form.
 * Every resulting conjunction is handed to Cassandra's planner unchanged; this class does not create native SAI
 * {@code Expression}s, fold ranges, or decide how indexed expressions should be intersected.</p>
 */
public class SaiQueryPlanner
{
    private SaiQueryPlanner()
    {
        throw new IllegalStateException("Static utility class");
    }

    @Nullable
    public static Plan plan(@NotNull TableMetadata metadata, @NotNull List<SaiFilter> filters)
    {
        List<List<SaiFilter>> clauses = conjunctiveClauses(filters);
        if (clauses == null || clauses.isEmpty())
        {
            return null;
        }

        Map<String, StorageAttachedIndex> indexesCache = new HashMap<>();

        List<Conjunction> conjunctions = new ArrayList<>(clauses.size());
        Map<StorageAttachedIndex, StorageAttachedIndex> resourceIndexes = new LinkedHashMap<>();
        for (List<SaiFilter> clause : clauses)
        {
            Conjunction conjunction = conjunction(indexesCache, metadata, clause);
            if (conjunction == null)
            {
                return null;
            }
            conjunctions.add(conjunction);
            for (StorageAttachedIndex index : conjunction.indexesByColumn.values())
            {
                resourceIndexes.putIfAbsent(index, index);
            }
        }
        return new Plan(conjunctions, new ArrayList<>(resourceIndexes.values()));
    }

    /**
     * Converts the top-level implicit AND plus any nested boolean nodes to a bounded DNF.
     *
     * @return list of AND clauses
     */
    @Nullable
    static List<List<SaiFilter>> conjunctiveClauses(@NotNull List<SaiFilter> filters)
    {
        List<List<SaiFilter>> result = new ArrayList<>();
        result.add(new ArrayList<>());
        for (SaiFilter filter : filters)
        {
            List<List<SaiFilter>> next = conjunctiveClauses(filter);
            if (next == null)
            {
                return null;
            }
            result = and(result, next);
            if (result == null)
            {
                return null;
            }
        }
        return result;
    }

    @Nullable
    private static List<List<SaiFilter>> conjunctiveClauses(@NotNull SaiFilter filter)
    {
        if (filter.isPredicate())
        {
            List<SaiFilter> clause = new ArrayList<>(1);
            clause.add(filter);
            List<List<SaiFilter>> clauses = new ArrayList<>(1);
            clauses.add(clause);
            return clauses;
        }

        List<List<SaiFilter>> left = conjunctiveClauses(filter.left());
        List<List<SaiFilter>> right = conjunctiveClauses(filter.right());
        if (left == null || right == null)
        {
            return null;
        }

        if (filter.kind() == SaiFilter.Kind.OR)
        {
            List<List<SaiFilter>> clauses = new ArrayList<>(left.size() + right.size());
            clauses.addAll(left);
            clauses.addAll(right);
            return clauses;
        }
        return and(left, right);
    }

    @Nullable
    private static List<List<SaiFilter>> and(@NotNull List<List<SaiFilter>> left,
                                             @NotNull List<List<SaiFilter>> right)
    {
        if (left.isEmpty() || right.isEmpty())
        {
            return Collections.emptyList();
        }

        List<List<SaiFilter>> clauses = new ArrayList<>(left.size() * right.size());
        for (List<SaiFilter> leftClause : left)
        {
            for (List<SaiFilter> rightClause : right)
            {
                List<SaiFilter> clause = new ArrayList<>(leftClause.size() + rightClause.size());
                clause.addAll(leftClause);
                clause.addAll(rightClause);
                clauses.add(clause);
            }
        }
        return clauses;
    }

    @Nullable
    private static Conjunction conjunction(@NotNull Map<String, StorageAttachedIndex> indexesCache,
                                           @NotNull TableMetadata metadata,
                                           @NotNull List<SaiFilter> predicates)
    {
        if (predicates.isEmpty())
        {
            return null;
        }

        // These are offline SSTables and reconciliation still happens in the normal Analytics read path, so
        // Cassandra may safely use strict local index intersection for the candidate-key phase.
        RowFilter rowFilter = RowFilter.create(false);
        Map<ColumnMetadata, StorageAttachedIndex> indexesByColumn = new LinkedHashMap<>();
        for (SaiFilter predicate : predicates)
        {
            Preconditions.checkState(predicate.isPredicate(), "Disjunctive Normal Form clause contains a boolean SAI filter: " + predicate);

            StorageAttachedIndex index = storageAttachedIndex(indexesCache, metadata, predicate.index());
            Operator operator = operator(predicate.operator());
            if (!canSearch(index, operator))
            {
                return null;
            }

            ColumnMetadata column = index.termType().columnMetadata();
            StorageAttachedIndex previous = indexesByColumn.putIfAbsent(column, index);
            if (previous != null && previous != index)
            {
                // Cassandra's planner asks for one best SAI index per RowFilter expression. Avoid guessing when the
                // version-neutral plan somehow selected different indexes for the same column.
                // TODO(lantoniak): Log warning.
                return null;
            }
            rowFilter.add(column, operator, index.termType().fromString(predicate.value()));
        }
        return new Conjunction(rowFilter, indexesByColumn);
    }

    private static boolean canSearch(@NotNull StorageAttachedIndex index, @NotNull Operator operator)
    {
        return !index.hasAnalyzer()
               && !index.termType().isVector()
               && !index.termType().isNonFrozenCollection()
               && !index.termType().isFrozenCollection()
               && !index.termType().isComposite()
               && index.supportsExpression(index.termType().columnMetadata(), operator);
    }

    @NotNull
    private static StorageAttachedIndex storageAttachedIndex(@NotNull Map<String, StorageAttachedIndex> indexesCache,
                                                             @NotNull TableMetadata metadata,
                                                             @NotNull SaiIndex saiIndex)
    {
        String cacheKey = metadata.id + ":" + saiIndex.name();
        return indexesCache.computeIfAbsent(cacheKey, ignored -> {
            ColumnFamilyStore cfs = Keyspace.openWithoutSSTables(metadata.keyspace).getColumnFamilyStore(metadata.name);
            Map<String, String> options = new HashMap<>(saiIndex.options());
            options.put(IndexTarget.TARGET_OPTION_NAME, saiIndex.target());
            options.put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            IndexMetadata indexMetadata = IndexMetadata.fromSchemaMetadata(saiIndex.name(),
                                                                          IndexMetadata.Kind.CUSTOM,
                                                                          options);
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

    public static final class Plan
    {
        private final List<Conjunction> conjunctions;
        private final List<StorageAttachedIndex> resourceIndexes;

        private Plan(@NotNull List<Conjunction> conjunctions,
                     @NotNull List<StorageAttachedIndex> resourceIndexes)
        {
            this.conjunctions = Collections.unmodifiableList(new ArrayList<>(conjunctions));
            this.resourceIndexes = Collections.unmodifiableList(new ArrayList<>(resourceIndexes));
        }

        @NotNull
        public List<Conjunction> conjunctions()
        {
            return conjunctions;
        }

        @NotNull
        public List<StorageAttachedIndex> resourceIndexes()
        {
            return resourceIndexes;
        }
    }

    public static final class Conjunction
    {
        private final RowFilter rowFilter;
        private final Map<ColumnMetadata, StorageAttachedIndex> indexesByColumn;

        private Conjunction(@NotNull RowFilter rowFilter,
                            @NotNull Map<ColumnMetadata, StorageAttachedIndex> indexesByColumn)
        {
            this.rowFilter = rowFilter;
            this.indexesByColumn = Collections.unmodifiableMap(new LinkedHashMap<>(indexesByColumn));
        }

        @NotNull
        public RowFilter rowFilter()
        {
            return rowFilter;
        }

        @Nullable
        public StorageAttachedIndex indexFor(@NotNull ColumnMetadata column)
        {
            return indexesByColumn.get(column);
        }
    }
}
