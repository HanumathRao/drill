/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl;

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClientFixture;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestOrderedMuxExchange extends PlanTestBase {

  private final static String ORDERED_MUX_EXCHANGE = "OrderedMuxExchange";


  private void validateResults(BufferAllocator allocator, List<QueryDataBatch> results) throws SchemaChangeException {
    long previousBigInt = Long.MIN_VALUE;

    for (QueryDataBatch b : results) {
      RecordBatchLoader loader = new RecordBatchLoader(allocator);
      if (b.getHeader().getRowCount() > 0) {
        loader.load(b.getHeader().getDef(),b.getData());
        @SuppressWarnings({ "deprecation", "resource" })
        IntVector c1 = (IntVector) loader.getValueAccessorById(IntVector.class,
                   loader.getValueVectorId(new SchemaPath("id_i", ExpressionPosition.UNKNOWN)).getFieldIds()).getValueVector();
        IntVector.Accessor a1 = c1.getAccessor();

        for (int i = 0; i < c1.getAccessor().getValueCount(); i++) {
          assertTrue(String.format("%d > %d", previousBigInt, a1.get(i)), previousBigInt <= a1.get(i));
          previousBigInt = a1.get(i);
        }
      }
      loader.clear();
      b.release();
    }
  }

  /**
   * Test case to verify the OrderedMuxExchange created for order by clause.
   * It checks by forcing the plan to create OrderedMuxExchange and also verifies the
   * output column is ordered.
   *
   * @throws Exception if anything goes wrong
   */

  @Test
  public void testOrderedMuxForOrderBy() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
            .maxParallelization(1)
            .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
            ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      client.alterSession(ExecConstants.SLICE_TARGET, 10);
      String sql = "SELECT id_i, name_s10 FROM `mock`.`employees_10K` ORDER BY id_i limit 10";

      String explainText = client.queryBuilder().sql(sql).explainText();
      assertTrue(explainText.contains(ORDERED_MUX_EXCHANGE));
      validateResults(client.allocator(), client.queryBuilder().sql(sql).results());
    }
  }

  /**
   * Test case to verify the OrderedMuxExchange created for window functions.
   * It checks by forcing the plan to create OrderedMuxExchange and also verifies the
   * output column is ordered.
   *
   * @throws Exception if anything goes wrong
   */

  @Test
  public void testOrderedMuxForWindowAgg() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
            .maxParallelization(1)
            .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
            ;

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      client.alterSession(ExecConstants.SLICE_TARGET, 10);
      String sql = "SELECT id_i, max(name_s10) over(order by id_i) FROM `mock`.`employees_10K` limit 10";

      String explainText = client.queryBuilder().sql(sql).explainText();
      assertTrue(explainText.contains(ORDERED_MUX_EXCHANGE));
      validateResults(client.allocator(), client.queryBuilder().sql(sql).results());
    }
  }
}
