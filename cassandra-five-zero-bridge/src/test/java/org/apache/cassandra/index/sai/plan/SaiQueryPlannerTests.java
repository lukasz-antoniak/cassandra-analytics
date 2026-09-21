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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.SaiIndex;
import org.apache.cassandra.spark.sparksql.filters.SaiFilter;

import static org.assertj.core.api.Assertions.assertThat;

class SaiQueryPlannerTests
{
    private static final SaiIndex INDEX = new SaiIndex("value_idx", "value", "value", Collections.emptyMap());

    @Test
    void testConvertsSparkOrToConjunctionsForNativePlanner()
    {
        SaiFilter a = predicate("a");
        SaiFilter b = predicate("b");
        SaiFilter c = predicate("c");

        List<List<SaiFilter>> clauses =
        SaiQueryPlanner.conjunctiveClauses(Arrays.asList(a, SaiFilter.or(b, c)));

        assertThat(clauses).containsExactly(Arrays.asList(a, b), Arrays.asList(a, c));
    }

    @Test
    void testDistributesNestedAndOverOr()
    {
        SaiFilter a = predicate("a");
        SaiFilter b = predicate("b");
        SaiFilter c = predicate("c");
        SaiFilter d = predicate("d");

        List<List<SaiFilter>> clauses =
        SaiQueryPlanner.conjunctiveClauses(Collections.singletonList(SaiFilter.and(SaiFilter.or(a, b),
                                                                                   SaiFilter.or(c, d))));

        assertThat(clauses).containsExactly(Arrays.asList(a, c),
                                            Arrays.asList(a, d),
                                            Arrays.asList(b, c),
                                            Arrays.asList(b, d));
    }

    private static SaiFilter predicate(String value)
    {
        return new SaiFilter(INDEX, SaiFilter.Operator.EQ, value);
    }
}
