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
package org.apache.drill.exec.planner.index.rules;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.index.IndexCollection;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.logical.partition.RewriteAsBinaryOperators;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FlattenToIndexScanPrule extends AbstractIndexPrule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenToIndexScanPrule.class);

  public static final RelOptRule FILTER_PROJECT_SCAN = new FlattenToIndexScanPrule(
      RelOptHelper.some(DrillFilterRel.class,
          RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
      "FlattenToIndexScanPrule:Filter_Project_Scan", new MatchFPS());

  final private MatchFunction<IndexLogicalPlanCallContext> match;

  private FlattenToIndexScanPrule(RelOptRuleOperand operand,
                                   String description,
                                   MatchFunction<IndexLogicalPlanCallContext> match) {
    super(operand, description);
    this.match = match;
  }

  private static class MatchFPS extends AbstractMatchFunction<IndexLogicalPlanCallContext> {

    Map<String, RexCall> flattenMap = Maps.newHashMap();

    public boolean match(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(2);
      final DrillProjectRel project = (DrillProjectRel) call.rel(1);
      if (checkScan(scan)) {
        boolean found = false;

        // if Project does not contain a FLATTEN expression, rule does not apply
        for (Pair<RexNode, String> p : project.getNamedProjects()) {
          if (p.left instanceof RexCall) {
            RexCall function = (RexCall) p.left;
            String functionName = function.getOperator().getName();
            if (functionName.equalsIgnoreCase("flatten")
                && function.getOperands().size() == 1) {
              flattenMap.put((String) p.right, (RexCall) p.left);
              found = true;
              // continue since there may be multiple FLATTEN exprs which may be
              // referenced by the filter condition
            }
          }
        }

        return found;
      }
      return false;
    }

    public IndexLogicalPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillFilterRel filter = call.rel(0);
      final DrillProjectRel project = call.rel(1);
      final DrillScanRel scan = call.rel(2);

      IndexLogicalPlanCallContext idxContext = new IndexLogicalPlanCallContext(call, null, filter, project, scan);
      idxContext.setFlattenMap(flattenMap);
      return idxContext;
    }

  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return match.match(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    doOnMatch((IndexLogicalPlanCallContext) match.onMatch(call));
  }

  private void doOnMatch(IndexLogicalPlanCallContext indexContext) {
    Stopwatch indexPlanTimer = Stopwatch.createStarted();
    final PlannerSettings settings = PrelUtil.getPlannerSettings(indexContext.call.getPlanner());

    DbGroupScan groupScan = (DbGroupScan)indexContext.scan.getGroupScan();

    final IndexCollection indexCollection = groupScan.getSecondaryIndexCollection(indexContext.scan);
    if (indexCollection == null) {
      return;
    }

    logger.debug("Index Rule {} starts", this.description);

    RexBuilder builder = indexContext.filter.getCluster().getRexBuilder();

    RexNode condition = null;
    if (indexContext.lowerProject == null) {
      condition = indexContext.filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushFilterPastProject(indexContext.filter.getCondition(), indexContext.lowerProject);
    }

    //save this pushed down condition, in case it is needed later to build filter when joining back primary table
    indexContext.origPushedCondition = condition;

    // for each conjunct in the Filter, check if the conjunct is referencing the FLATTEN output
    List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
    FilterVisitor  filterVisitor =
        new FilterVisitor(indexContext.getFlattenMap(), indexContext.lowerProject, builder);

//    for (RexNode n : conjuncts) {
//      flattenPath = n.accept(filterVisitor);
//      SchemaPath schema = SchemaPath.parseFrom(flattenPath);
//    }

    condition = condition.accept(filterVisitor);

    // the index analysis code only understands binary operators, so the condition should be
    // rewritten to convert N-ary ANDs and ORs into binary ANDs and ORs
    RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, builder);
    condition = condition.accept(visitor);

    if (indexCollection.supportsIndexSelection()) {
      // TODO: index selection and plan generation
    } else {
      throw new UnsupportedOperationException("Index collection must support index selection");
    }

    indexPlanTimer.stop();
    logger.debug("Index Planning took {} ms", indexPlanTimer.elapsed(TimeUnit.MILLISECONDS));
  }

  /**
   * Query:
   * select d from (select flatten(t1.`first`.`second`.`a`) as f from t1) as t
   *    where t.f.b < 10 AND t.f.c > 20;
   * The logical plan has the following:
   *      DrillFilterRel: condition=[AND(<(ITEM($0, 'b'), 10), >(ITEM($0, 'c'), 20))])
   *      DrillProjectRel: FLATTEN(ITEM(ITEM($1, 'second'), 'a')
   *
   *
   *                   AND
   *                 /       \
   *                /         \
   *               <           >
   *              /  \        / \
   *            ITEM  10   ITEM  20
   *           /    \      /  \
   *          $0    'b'   $0  'c'
   *          |          /
   *      FLATTEN-------/
   *        |
   *      ITEM
   *      /  \
   *    ITEM  'a'
   *    /  \
   *  $1  'second'
   *  |
   *  Scan's RowType: ['first']
   *
   */
  private static class FilterVisitor extends RexVisitorImpl<RexNode> {

    private final Map<String, RexCall> flattenMap;
    private final DrillProjectRel project;
    private final RexBuilder builder;

    FilterVisitor(Map<String, RexCall> flattenMap, DrillProjectRel project,
        RexBuilder builder) {
      super(true);
      this.project = project;
      this.flattenMap = flattenMap;
      this.builder = builder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      SqlOperator op = call.getOperator();
      SqlKind kind = op.getKind();
      RelDataType type = call.getType();

      if (kind == SqlKind.OR || kind == SqlKind.AND) {
        return builder.makeCall(type, op, visitChildren(call));
      } else if (SqlStdOperatorTable.ITEM.equals(op) &&
          call.getOperands().size() == 2) {
        if (call.getOperands().get(0) instanceof RexInputRef) {
          RexInputRef inputRef = (RexInputRef) call.getOperands().get(0);

          // check if input is referencing a FLATTEN
          String projectFieldName = project.getRowType().getFieldNames().get(inputRef.getIndex());
          RexCall c;
          if ((c = flattenMap.get(projectFieldName)) != null) {
            // take the Flatten's input and build a new RexExpr with ITEM($n, -1)
            RexNode left = c.getOperands().get(0);
            RexLiteral right = builder.makeBigintLiteral(BigDecimal.valueOf(-1));
            RexNode result = builder.makeCall(call.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));
            return result;
          }
        }
      }
      return super.visitCall(call);
    }

    private List<RexNode> visitChildren(RexCall call) {
      List<RexNode> children = Lists.newArrayList();
      for (RexNode child : call.getOperands()) {
        children.add(child.accept(this));
      }
      return ImmutableList.copyOf(children);
    }
  }
}
