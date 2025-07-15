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

package org.apache.cassandra.spark.endtoend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.characters;
import static org.quicktheories.generators.SourceDSL.integers;

@Tag("Sequential")
public class TombstoneTests
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testPartitionTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 100;
        int numColumns = 10;
        qt().withExamples(20)
            .forAll(integers().between(0, numRows - 2))
            .checkAssert(deleteRangeStart -> {
                assert 0 <= deleteRangeStart && deleteRangeStart < numRows;
                int deleteRangeEnd = deleteRangeStart + RandomUtils.RANDOM.nextInt(numRows - deleteRangeStart - 1) + 1;
                assert deleteRangeStart < deleteRangeEnd && deleteRangeEnd < numRows;

                Tester.builder(TestSchema.basicBuilder(bridge)
                                         .withDeleteFields("a ="))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  writer.write(row, column, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = deleteRangeStart; row < deleteRangeEnd; row++)
                          {
                              writer.write(row);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              int value = row.getInt(0);
                              assertTrue(0 <= value && value < numRows);
                              assertTrue(value < deleteRangeStart || value >= deleteRangeEnd);
                              count++;
                          }
                          assertEquals((numRows - (deleteRangeEnd - deleteRangeStart)) * numColumns, count);
                      })
                      .run(bridge.getVersion());
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRowTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 100;
        int numColumns = 10;
        qt().withExamples(20)
            .forAll(integers().between(0, numColumns - 1))
            .checkAssert(colNum ->
                         Tester.builder(TestSchema.basicBuilder(bridge)
                                                  .withDeleteFields("a =", "b ="))
                               .withVersions(TestUtils.tombstoneTestableVersions())
                               .dontWriteRandomData()
                               .withSSTableWriter(writer -> {
                                   for (int row = 0; row < numRows; row++)
                                   {
                                       for (int column = 0; column < numColumns; column++)
                                       {
                                           writer.write(row, column, column);
                                       }
                                   }
                               })
                               .withTombstoneWriter(writer -> {
                                   for (int row = 0; row < numRows; row++)
                                   {
                                       writer.write(row, colNum);
                                   }
                               })
                               .dontCheckNumSSTables()
                               .withCheck(dataset -> {
                                   int count = 0;
                                   for (Row row : dataset.collectAsList())
                                   {
                                       int value = row.getInt(0);
                                       assertTrue(row.getInt(0) >= 0 && value < numRows);
                                       assertTrue(row.getInt(1) != colNum);
                                       assertEquals(row.get(1), row.get(2));
                                       count++;
                                   }
                                   assertEquals(numRows * (numColumns - 1), count);
                               })
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRangeTombstoneInt(CassandraBridge bridge)
    {
        int numRows = 10;
        int numColumns = 128;
        qt().withExamples(10)
            .forAll(integers().between(0, numColumns - 1))
            .checkAssert(startBound -> {
                assertTrue(startBound < numColumns);
                int endBound = startBound + RandomUtils.RANDOM.nextInt(numColumns - startBound);
                assertTrue(endBound >= startBound && endBound <= numColumns);
                int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.basicBuilder(bridge)
                                         .withDeleteFields("a =", "b >=", "b <"))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  writer.write(row, column, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              writer.write(row, startBound, endBound);
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              // Verify row values exist within correct range with range tombstoned values removed
                              int value = row.getInt(1);
                              assertEquals(value, row.getInt(2));
                              assertTrue(value <= numColumns);
                              assertTrue(value < startBound || value >= endBound);
                              count++;
                          }
                          assertEquals(numRows * (numColumns - numTombstones), count);
                      })
                      .run(bridge.getVersion());
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testRangeTombstoneString(CassandraBridge bridge)
    {
        int numRows = 10;
        int numColumns = 128;
        qt().withExamples(10)
            .forAll(characters().ascii())
            .checkAssert(startBound -> {
                assertTrue(startBound <= numColumns);
                char endBound = (char) (startBound + RandomUtils.RANDOM.nextInt(numColumns - startBound));
                assertTrue(endBound >= startBound && endBound <= numColumns);
                int numTombstones = endBound - startBound;

                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("a", bridge.aInt())
                                         .withClusteringKey("b", bridge.text())
                                         .withColumn("c", bridge.aInt())
                                         .withDeleteFields("a =", "b >=", "b <"))
                      .withVersions(TestUtils.tombstoneTestableVersions())
                      .dontWriteRandomData()
                      .withSSTableWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              for (int column = 0; column < numColumns; column++)
                              {
                                  String value = String.valueOf((char) column);
                                  writer.write(row, value, column);
                              }
                          }
                      })
                      .withTombstoneWriter(writer -> {
                          for (int row = 0; row < numRows; row++)
                          {
                              writer.write(row, startBound.toString(), Character.toString(endBound));
                          }
                      })
                      .dontCheckNumSSTables()
                      .withCheck(dataset -> {
                          int count = 0;
                          for (Row row : dataset.collectAsList())
                          {
                              // Verify row values exist within correct range with range tombstoned values removed
                              char character = row.getString(1).charAt(0);
                              assertTrue(character <= numColumns);
                              assertTrue(character < startBound || character >= endBound);
                              count++;
                          }
                          assertEquals(numRows * (numColumns - numTombstones), count);
                      })
                      .run(bridge.getVersion());
            });
    }
}
