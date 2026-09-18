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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.SaiIndex;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Translates the version-neutral {@link SaiFilter} tree into the Cassandra 5 SAI expressions that
 * {@link SaiIndexReader} executes against the open SSTable indexes.
 *
 * <p>The planner deliberately performs only query-shape planning. It does not open SAI components,
 * search SSTables, or compose results across SSTables. In particular, the UNION-across-SSTables
 * correctness rule belongs to {@link SaiIndexReader}, which owns execution.</p>
 *
 * <p>Conjunctive predicates targeting the same SAI index are folded into one native
 * {@link Expression}. For example, {@code value >= 10 AND value < 20} becomes one bounded SAI
 * expression instead of two independent searches followed by an intersection. OR boundaries are
 * preserved because folding across them would change boolean semantics.</p>
 */
final class SaiQueryPlanner
{
    private static final Map<String, StorageAttachedIndex> INDEXES = new ConcurrentHashMap<>();

    private SaiQueryPlanner()
    {
        throw new IllegalStateException("Static utility class");
    }

    @Nullable
    static Plan plan(@NotNull TableMetadata metadata, @NotNull List<SaiFilter> filters)
    {
        return buildAndPlan(metadata, filters);
    }

    @Nullable
    private static Plan buildPlan(@NotNull TableMetadata metadata, @NotNull SaiFilter filter)
    {
        if (filter.kind() == SaiFilter.Kind.AND)
        {
            List<SaiFilter> conjuncts = new ArrayList<>();
            collectConjuncts(filter, conjuncts);
            return buildAndPlan(metadata, conjuncts);
        }
        if (filter.kind() == SaiFilter.Kind.OR)
        {
            Plan left = buildPlan(metadata, filter.left());
            Plan right = buildPlan(metadata, filter.right());
            return left == null || right == null
                   ? null
                   : combine(Plan.Kind.OR, Arrays.asList(left, right));
        }
        return buildPredicatePlan(metadata, Collections.singletonList(filter));
    }

    @Nullable
    private static Plan buildAndPlan(@NotNull TableMetadata metadata, @NotNull List<SaiFilter> filters)
    {
        List<SaiFilter> conjuncts = new ArrayList<>();
        for (SaiFilter filter : filters)
        {
            collectConjuncts(filter, conjuncts);
        }

        Map<SaiIndex, List<SaiFilter>> groupedPredicates = new LinkedHashMap<>();
        List<SaiFilter> complexTerms = new ArrayList<>();
        for (SaiFilter conjunct : conjuncts)
        {
            if (conjunct.isPredicate())
            {
                groupedPredicates.computeIfAbsent(conjunct.index(), ignored -> new ArrayList<>()).add(conjunct);
            }
            else
            {
                complexTerms.add(conjunct);
            }
        }

        List<Plan> children = new ArrayList<>(groupedPredicates.size() + complexTerms.size());
        for (List<SaiFilter> predicates : groupedPredicates.values())
        {
            Plan predicate = buildPredicatePlan(metadata, predicates);
            if (predicate == null)
            {
                return null;
            }
            children.add(predicate);
        }
        for (SaiFilter complexTerm : complexTerms)
        {
            Plan child = buildPlan(metadata, complexTerm);
            if (child == null)
            {
                return null;
            }
            children.add(child);
        }
        return children.isEmpty() ? null : combine(Plan.Kind.AND, children);
    }

    private static void collectConjuncts(@NotNull SaiFilter filter, @NotNull List<SaiFilter> conjuncts)
    {
        if (filter.kind() == SaiFilter.Kind.AND)
        {
            collectConjuncts(filter.left(), conjuncts);
            collectConjuncts(filter.right(), conjuncts);
        }
        else
        {
            conjuncts.add(filter);
        }
    }

    @Nullable
    private static Plan buildPredicatePlan(@NotNull TableMetadata metadata, @NotNull List<SaiFilter> filters)
    {
        if (filters.isEmpty())
        {
            return null;
        }

        SaiIndex saiIndex = filters.get(0).index();
        StorageAttachedIndex index = storageAttachedIndex(metadata, saiIndex);
        if (!canSearch(index, filters))
        {
            return null;
        }

        Expression expression = Expression.create(index);
        for (SaiFilter filter : filters)
        {
            expression.add(operator(filter.operator()), index.termType().fromString(filter.value()));
        }
        return Plan.predicate(new IndexPlan(index, expression));
    }

    @NotNull
    private static Plan combine(@NotNull Plan.Kind kind, @NotNull List<Plan> children)
    {
        return children.size() == 1 ? children.get(0) : Plan.booleanNode(kind, children);
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
    private static StorageAttachedIndex storageAttachedIndex(@NotNull TableMetadata metadata,
                                                             @NotNull SaiIndex saiIndex)
    {
        String cacheKey = metadata.id + ":" + saiIndex.name();
        return INDEXES.computeIfAbsent(cacheKey, ignored -> {
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

    static final class Plan
    {
        enum Kind
        {
            PREDICATE,
            AND,
            OR
        }

        private final Kind kind;
        @Nullable
        private final IndexPlan predicate;
        private final List<Plan> children;

        private Plan(@NotNull Kind kind, @Nullable IndexPlan predicate, @NotNull List<Plan> children)
        {
            this.kind = kind;
            this.predicate = predicate;
            this.children = children;
        }

        @NotNull
        private static Plan predicate(@NotNull IndexPlan predicate)
        {
            return new Plan(Kind.PREDICATE, predicate, Collections.emptyList());
        }

        @NotNull
        private static Plan booleanNode(@NotNull Kind kind, @NotNull List<Plan> children)
        {
            return new Plan(kind, null, Collections.unmodifiableList(new ArrayList<>(children)));
        }

        boolean isPredicate()
        {
            return kind == Kind.PREDICATE;
        }

        boolean isOr()
        {
            return kind == Kind.OR;
        }

        @NotNull
        IndexPlan predicate()
        {
            if (predicate == null)
            {
                throw new IllegalStateException("Boolean SAI plan has no predicate");
            }
            return predicate;
        }

        @NotNull
        List<Plan> children()
        {
            return children;
        }

        @NotNull
        List<IndexPlan> resourcePlans()
        {
            Map<StorageAttachedIndex, IndexPlan> plans = new LinkedHashMap<>();
            collectResourcePlans(plans);
            return new ArrayList<>(plans.values());
        }

        private void collectResourcePlans(@NotNull Map<StorageAttachedIndex, IndexPlan> plans)
        {
            if (isPredicate())
            {
                IndexPlan plan = predicate();
                plans.putIfAbsent(plan.index(), plan);
                return;
            }
            for (Plan child : children)
            {
                child.collectResourcePlans(plans);
            }
        }
    }

    static final class IndexPlan
    {
        private final StorageAttachedIndex index;
        private final Expression expression;

        private IndexPlan(@NotNull StorageAttachedIndex index, @NotNull Expression expression)
        {
            this.index = index;
            this.expression = expression;
        }

        @NotNull
        StorageAttachedIndex index()
        {
            return index;
        }

        @NotNull
        Expression expression()
        {
            return expression;
        }
    }
}
