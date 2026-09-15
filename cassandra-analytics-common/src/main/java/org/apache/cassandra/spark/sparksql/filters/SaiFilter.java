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

package org.apache.cassandra.spark.sparksql.filters;

import java.io.Serializable;
import java.util.Objects;

import org.apache.cassandra.spark.data.SaiIndex;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Version-neutral SAI predicate tree used as an SSTable pruning hint.
 *
 * The Spark predicate is deliberately retained as an unsupported filter so Spark
 * evaluates it again after the SSTable read. SAI therefore narrows physical I/O
 * without becoming part of the correctness boundary.
 */
public final class SaiFilter implements Serializable
{
    private static final long serialVersionUID = 1L;

    public enum Kind
    {
        PREDICATE,
        AND,
        OR
    }

    public enum Operator
    {
        EQ,
        LT,
        LTE,
        GT,
        GTE
    }

    @NotNull
    private final Kind kind;
    @Nullable
    private final SaiIndex index;
    @Nullable
    private final Operator operator;
    @Nullable
    private final String value;
    @Nullable
    private final SaiFilter left;
    @Nullable
    private final SaiFilter right;

    /** Creates a leaf SAI predicate. */
    public SaiFilter(@NotNull SaiIndex index, @NotNull Operator operator, @NotNull String value)
    {
        this.kind = Kind.PREDICATE;
        this.index = index;
        this.operator = operator;
        this.value = value;
        this.left = null;
        this.right = null;
    }

    private SaiFilter(@NotNull Kind kind, @NotNull SaiFilter left, @NotNull SaiFilter right)
    {
        if (kind == Kind.PREDICATE)
        {
            throw new IllegalArgumentException("Boolean SAI filter cannot use PREDICATE kind");
        }
        this.kind = kind;
        this.index = null;
        this.operator = null;
        this.value = null;
        this.left = left;
        this.right = right;
    }

    @NotNull
    public static SaiFilter and(@NotNull SaiFilter left, @NotNull SaiFilter right)
    {
        return new SaiFilter(Kind.AND, left, right);
    }

    @NotNull
    public static SaiFilter or(@NotNull SaiFilter left, @NotNull SaiFilter right)
    {
        return new SaiFilter(Kind.OR, left, right);
    }

    @NotNull
    public Kind kind()
    {
        return kind;
    }

    public boolean isPredicate()
    {
        return kind == Kind.PREDICATE;
    }

    @NotNull
    public SaiIndex index()
    {
        ensurePredicate();
        return Objects.requireNonNull(index);
    }

    @NotNull
    public Operator operator()
    {
        ensurePredicate();
        return Objects.requireNonNull(operator);
    }

    @NotNull
    public String value()
    {
        ensurePredicate();
        return Objects.requireNonNull(value);
    }

    @NotNull
    public SaiFilter left()
    {
        ensureBoolean();
        return Objects.requireNonNull(left);
    }

    @NotNull
    public SaiFilter right()
    {
        ensureBoolean();
        return Objects.requireNonNull(right);
    }

    private void ensurePredicate()
    {
        if (!isPredicate())
        {
            throw new IllegalStateException("SAI boolean node does not have predicate metadata: " + kind);
        }
    }

    private void ensureBoolean()
    {
        if (isPredicate())
        {
            throw new IllegalStateException("SAI predicate node does not have boolean children");
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (!(other instanceof SaiFilter))
        {
            return false;
        }
        SaiFilter that = (SaiFilter) other;
        return kind == that.kind
               && Objects.equals(index, that.index)
               && operator == that.operator
               && Objects.equals(value, that.value)
               && Objects.equals(left, that.left)
               && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, index, operator, value, left, right);
    }

    @Override
    public String toString()
    {
        if (isPredicate())
        {
            return "SaiFilter{" + "index=" + index + ", operator=" + operator + ", value='" + value + '\'' + '}';
        }
        return "SaiFilter{" + kind + ", left=" + left + ", right=" + right + '}';
    }
}
