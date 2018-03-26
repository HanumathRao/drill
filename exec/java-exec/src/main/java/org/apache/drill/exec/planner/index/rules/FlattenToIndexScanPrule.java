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

import com.google.common.base.Preconditions;
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
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexCollection;
import org.apache.drill.exec.planner.index.IndexConditionInfo;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.IndexGroup;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.IndexProperties;
import org.apache.drill.exec.planner.index.IndexSelector;
import org.apache.drill.exec.planner.index.generators.CoveringIndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.NonCoveringIndexPlanGenerator;
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
    final IndexCollection indexCollection = getIndexCollection(settings, indexContext.scan);

    if (indexCollection == null ||
        !indexCollection.supportsArrayIndexes()) {
      return;
    }

    logger.debug("Index Rule {} starts", this.description);

    RexBuilder builder = indexContext.filter.getCluster().getRexBuilder();

    /*
    RexNode condition = null;
    if (indexContext.lowerProject == null) {
      condition = indexContext.filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushFilterPastProject(indexContext.filter.getCondition(), indexContext.lowerProject);
    } */

    RexNode condition = indexContext.filter.getCondition();

    //save this pushed down condition, in case it is needed later to build filter when joining back primary table
    indexContext.origPushedCondition = condition;

    // for each conjunct in the Filter, check if the conjunct is referencing the FLATTEN output
    List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);
    FilterVisitor  filterVisitor =
        new FilterVisitor(indexContext.getFlattenMap(), indexContext.lowerProject, builder);

    condition = condition.accept(filterVisitor);

    // the index analysis code only understands binary operators, so the condition should be
    // rewritten to convert N-ary ANDs and ORs into binary ANDs and ORs
    RewriteAsBinaryOperators visitor = new RewriteAsBinaryOperators(true, builder);
    condition = condition.accept(visitor);

    if (indexCollection.supportsIndexSelection()) {
      try {
        processWithIndexSelection(indexContext, settings, condition, indexCollection, builder);
      } catch(Exception e) {
        logger.warn("Exception while doing index planning ", e);
      }
    } else {
      throw new UnsupportedOperationException("Index collection must support index selection");
    }

    indexPlanTimer.stop();
    logger.debug("Index Planning took {} ms", indexPlanTimer.elapsed(TimeUnit.MILLISECONDS));
  }

  public void processWithIndexSelection(
      IndexLogicalPlanCallContext indexContext,
      PlannerSettings settings,
      RexNode condition,
      IndexCollection collection,
      RexBuilder builder) {

    DrillScanRel scan = indexContext.scan;
    IndexConditionInfo.Builder infoBuilder = IndexConditionInfo.newBuilder(condition, collection, builder, indexContext.scan);

    if (!analyzeCondition(indexContext, collection, condition, builder, infoBuilder, logger)) {
      return;
    }

    if (!initializeStatistics(scan, settings, indexContext, condition, indexContext.isValidIndexHint, logger)) {
      return;
    }

    List<IndexGroup> coveringIndexes = Lists.newArrayList();
    List<IndexGroup> nonCoveringIndexes = Lists.newArrayList();
    List<IndexGroup> intersectIndexes = Lists.newArrayList();

    IndexSelector selector = createAndInitSelector(indexContext,
        collection,
        builder,
        logger);

    // get the candidate indexes based on selection
    selector.getCandidateIndexes(infoBuilder, coveringIndexes, nonCoveringIndexes, intersectIndexes);

    GroupScan primaryTableScan = indexContext.scan.getGroupScan();

    try {
      for (IndexGroup index : coveringIndexes) {
        IndexProperties indexProps = index.getIndexProps().get(0);
        IndexDescriptor indexDesc = indexProps.getIndexDesc();
        IndexGroupScan idxScan = indexDesc.getIndexGroupScan();
        FunctionalIndexInfo indexInfo = indexDesc.getFunctionalInfo();

        RexNode indexCondition = indexProps.getLeadingColumnsFilter();
        RexNode remainderCondition = indexProps.getTotalRemainderFilter();
        //Copy primary table statistics to index table
        idxScan.setStatistics(((DbGroupScan) scan.getGroupScan()).getStatistics());
        logger.info("index_plan_info: Generating covering index plan for index: {}, query condition {}", indexDesc.getIndexName(), indexCondition.toString());

        CoveringIndexPlanGenerator planGen = new CoveringIndexPlanGenerator(indexContext, indexInfo, idxScan,
            indexCondition, remainderCondition, builder, settings);

        planGen.go();
      }
    } catch (Exception e) {
      logger.warn("Exception while trying to generate covering index plan", e);
    }

    // Create non-covering index plans.

    //First, check if the primary table scan supports creating a restricted scan
    if (primaryTableScan instanceof DbGroupScan &&
        (((DbGroupScan) primaryTableScan).supportsRestrictedScan())) {
      try {
        for (IndexGroup index : nonCoveringIndexes) {
          IndexProperties indexProps = index.getIndexProps().get(0);
          IndexDescriptor indexDesc = indexProps.getIndexDesc();
          IndexGroupScan idxScan = indexDesc.getIndexGroupScan();

          RexNode indexCondition = indexProps.getLeadingColumnsFilter();
          RexNode remainderCondition = indexProps.getTotalRemainderFilter();

          // Combine the index and remainder conditions such that the total condition can be re-applied
          RexNode totalCondition = IndexPlanUtils.getTotalFilter(indexCondition, remainderCondition, builder);

          //Copy primary table statistics to index table
          idxScan.setStatistics(((DbGroupScan) primaryTableScan).getStatistics());
          logger.info("index_plan_info: Generating non-covering index plan for index: {}, query condition {}", indexDesc.getIndexName(), indexCondition.toString());
          NonCoveringIndexPlanGenerator planGen = new NonCoveringIndexPlanGenerator(indexContext, indexDesc,
            idxScan, indexCondition, remainderCondition, totalCondition, builder, settings);
          planGen.go();
        }
      } catch (Exception e) {
        logger.warn("Exception while trying to generate non-covering index access plan", e);
      }
    }

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
      RelDataType type = call.getType();

      if (SqlStdOperatorTable.ITEM.equals(op) &&
          call.getOperands().size() == 2) {
        if (call.getOperands().get(0) instanceof RexInputRef) {
          RexInputRef inputRef = (RexInputRef) call.getOperands().get(0);
          RexLiteral literal = (RexLiteral) call.getOperands().get(1);

          // check if input is referencing a FLATTEN
          String projectFieldName = project.getRowType().getFieldNames().get(inputRef.getIndex());
          RexCall c;
          RexNode left = null;
          if ((c = flattenMap.get(projectFieldName)) != null) {
            // drill down to the left leaf level of the Flatten to find which field it is
            // referencing from the Scan
/*
            RexNode n = c;
            while (true) {
              // here we are only expecting a RexCall or a RexInputRef
              if (n instanceof RexCall) {
                n = ((RexCall)n).getOperands().get(0);
              }
              if (n instanceof RexInputRef) {
                left = n;
                break;
              }
            }
*/
            left = c.getOperands().get(0);
            Preconditions.checkArgument(left != null, "Found null input reference for Flatten") ;

            // take the Flatten's input and build a new RexExpr with ITEM($n, -1)
            RexLiteral right = builder.makeBigintLiteral(BigDecimal.valueOf(-1));
            RexNode result1 = builder.makeCall(call.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));

            // final output is ITEM(ITEM($n, -1), 'literal')
            left = result1;
            right = literal;
            RexNode result2 = builder.makeCall(call.getType(), SqlStdOperatorTable.ITEM, ImmutableList.of(left, right));
            return result2;
          }
        }
      }
      return builder.makeCall(type, op, visitChildren(call));
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      return inputRef;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      return literal;
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
