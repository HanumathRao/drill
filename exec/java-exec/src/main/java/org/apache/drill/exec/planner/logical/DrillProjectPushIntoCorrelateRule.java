package org.apache.drill.exec.planner.logical;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.collections.map.HashedMap;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class DrillProjectPushIntoCorrelateRule  extends RelOptRule {

  public static final DrillProjectPushIntoCorrelateRule INSTANCE =
    new DrillProjectPushIntoCorrelateRule(RelFactories.LOGICAL_BUILDER);


  public DrillProjectPushIntoCorrelateRule(RelBuilderFactory relFactory) {
    super(operand(DrillProjectRel.class,
        operand(DrillCorrelateRel.class, any())),
      relFactory, null);
  }

  public void onMatch(RelOptRuleCall call) {
    DrillProjectRel origProj = call.rel(0);
    final DrillCorrelateRel corr = call.rel(1);

    if (StarColumnHelper.containsStarColumn(origProj.getRowType()) ||
        StarColumnHelper.containsStarColumn(corr.getRowType()) ||
         corr.donotIncludeCorrelateVariable) {
      return;
    }
    DrillRelOptUtil.InputRefVisitor collectRefs = new DrillRelOptUtil.InputRefVisitor();
    for (RexNode exp: origProj.getChildExps()) {
      exp.accept(collectRefs);
    }

    int correlationIndex = corr.getRequiredColumns().nextSetBit(0);
    for (RexInputRef inputRef : collectRefs.getInputRefs()) {
      if (inputRef.getIndex() == correlationIndex) {
        return;
      }
    }

    final RelNode left = corr.getLeft();
    final RelNode right = corr.getRight();
    final RelNode convertedLeft = convert(left, left.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
    final RelNode convertedRight = convert(right, right.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());

    final RelTraitSet traits = corr.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    RelNode relNode = new DrillCorrelateRel(corr.getCluster(),
                            traits, convertedLeft, convertedRight, true, corr.getCorrelationId(),
                            corr.getRequiredColumns(), corr.getJoinType());

    if (!DrillRelOptUtil.isTrivialProject(origProj, true)) {
      Map<Integer, Integer> mapWithoutCorr = buildMapWithoutCorrColumn(corr, correlationIndex);
      List<RexNode> outputExprs = transformExprs(origProj.getCluster().getRexBuilder(), origProj.getChildExps(), mapWithoutCorr);

      relNode = new DrillProjectRel(origProj.getCluster(),
                                    left.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
                                    relNode, outputExprs, origProj.getRowType());
    }
    call.transformTo(relNode);
  }

  private List<RexNode> transformExprs(RexBuilder builder, List<RexNode> exprs, Map<Integer, Integer> corrMap) {
    List<RexNode> outputExprs = new ArrayList<>();
    DrillRelOptUtil.RexFieldsTransformer transformer = new DrillRelOptUtil.RexFieldsTransformer(builder, corrMap);
    for (RexNode expr : exprs) {
      outputExprs.add(transformer.go(expr));
    }
    return outputExprs;
  }

  private Map<Integer, Integer> buildMapWithoutCorrColumn(RelNode corr, int correlationIndex) {
    int index = 0;
    Map<Integer, Integer> result = new HashedMap();
    for (int i=0;i<corr.getRowType().getFieldList().size();i++) {
      if (i == correlationIndex) {
        continue;
      } else {
        result.put(i, index++);
      }
    }
    return result;
  }
}