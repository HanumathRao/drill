/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.index.generators;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.calcite.rel.RelNode;
/**
 * Generate a Non covering index plan that is semantically equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class SemiJoinMergeRowKeyJoinGenerator extends NonCoveringIndexPlanGenerator {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinMergeRowKeyJoinGenerator.class);

  public SemiJoinMergeRowKeyJoinGenerator(SemiJoinIndexPlanCallContext indexContext,
                                       IndexDescriptor indexDesc,
                                       IndexGroupScan indexGroupScan,
                                       RexNode indexCondition,
                                       RexNode remainderCondition,
                                       RexNode totalCondition,
                                       RexBuilder builder,
                                       PlannerSettings settings) {
    super(indexContext.rightSide, indexDesc, indexGroupScan,
            indexCondition, remainderCondition, totalCondition, builder, settings);
  }

  @Override
  public RelNode convertChild(final RelNode filter, final RelNode input) throws InvalidRelException {
    RelNode coveringIndexScanRel = super.convertChild(filter, input);
    //build HashAgg for distinct processing
    //build RowKeyJoin and restricted scan and also merge the rowkey join.
    return coveringIndexScanRel;
  }
}
