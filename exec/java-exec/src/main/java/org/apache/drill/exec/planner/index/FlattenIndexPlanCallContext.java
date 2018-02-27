/*
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
package org.apache.drill.exec.planner.index;

import java.util.Map;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;

public class FlattenIndexPlanCallContext extends IndexLogicalPlanCallContext {

  /**
   * Filter directly above the Flatten's project (this filter cannot be pushed down)
   */
  protected DrillFilterRel filterAboveFlatten = null;

  /**
   * Filter below the Flatten (and above the Scan)
   */
  protected DrillFilterRel filterBelowFlatten = null;

  protected Map<String, RexCall> flattenMap = null;

  public FlattenIndexPlanCallContext(RelOptRuleCall call,
      DrillProjectRel upperProject,
      DrillFilterRel filterAboveFlatten,
      DrillProjectRel projectWithFlatten,
      DrillFilterRel filterBelowFlatten,
      DrillScanRel scan,
      Map<String, RexCall> flattenMap) {
    super(call, null /* no Sort */,
        upperProject,
        filterAboveFlatten,
        projectWithFlatten,
        scan);

    this.filterAboveFlatten = filterAboveFlatten;
    this.filterBelowFlatten = filterBelowFlatten;
    this.flattenMap = flattenMap;
  }

  public Map<String, RexCall> getFlattenMap() {
    return flattenMap;
  }

  public DrillFilterRel getFilterAboveFlatten() {
    return filterAboveFlatten;
  }

  public DrillFilterRel getFilterBelowFlatten() {
    return filterBelowFlatten;
  }

}
