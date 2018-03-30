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

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.exec.physical.base.IndexGroupScan;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.exec.planner.index.FlattenIndexPlanCallContext;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.generators.SemiJoinMergeRowKeyJoinGenerator;
import org.apache.drill.exec.planner.index.generators.SemiJoinToRowKeyJoinGenerator;
import org.apache.drill.exec.planner.index.generators.SemiJoinToCoveringIndexScanGenerator;
import org.apache.drill.exec.planner.index.generators.IndexPlanGenerator;
import org.apache.drill.exec.planner.index.generators.AbstractIndexPlanGenerator;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.PlannerSettings;

import java.util.List;
import java.util.Map;

public class SemiJoinIndexScanPrule extends AbstractIndexPrule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SemiJoinIndexScanPrule.class);

  public static final RelOptRule JOIN_FILTER_PROJECT_SCAN = new SemiJoinIndexScanPrule(
          RelOptHelper.some(DrillJoinRel.class,
                  RelOptHelper.some(DrillAggregateRel.class,
                          RelOptHelper.some(DrillProjectRel.class,
                                FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.getOperand())),
                  RelOptHelper.any(DrillScanRel.class)),
          "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Scan", new MatchJSPFPS());

  public static final RelOptRule JOIN_FILTER_PROJECT_SCAN_OPP = new SemiJoinIndexScanPrule(
          RelOptHelper.some(DrillJoinRel.class,
                  RelOptHelper.any(DrillScanRel.class),
                  RelOptHelper.some(DrillAggregateRel.class,
                          RelOptHelper.some(DrillProjectRel.class,
                                  FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.getOperand()))),
          "SemiJoinIndexScanPrule:Join_Project_Filter_Project_Scan_opp", new MatchJSPFPS());

  final private MatchFunction<SemiJoinIndexPlanCallContext> match;

  private SemiJoinIndexScanPrule(RelOptRuleOperand operand,
                                 String description,
                                 MatchFunction<SemiJoinIndexPlanCallContext> match) {
    super(operand, description);
    this.match = match;
  }

  private static class SemiJoinCoveringIndexPlanGenerator implements IndexPlanGenerator {
    private final SemiJoinIndexPlanCallContext context;

    private SemiJoinCoveringIndexPlanGenerator(SemiJoinIndexPlanCallContext context) {
      this.context = context;
    }

    @Override
    public AbstractIndexPlanGenerator getCoveringIndexGen(FunctionalIndexInfo functionInfo,
                                                          IndexGroupScan indexGroupScan,
                                                          RexNode indexCondition,
                                                          RexNode remainderCondition,
                                                          RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinToCoveringIndexScanGenerator(context, functionInfo, indexGroupScan, indexCondition, remainderCondition, builder, settings);
    }

    @Override
    public AbstractIndexPlanGenerator getNonCoveringIndexGen(IndexDescriptor indexDesc,
                                                             IndexGroupScan indexGroupScan,
                                                             RexNode indexCondition,
                                                             RexNode remainderCondition,
                                                             RexNode totalCondition,
                                                             RexBuilder builder, PlannerSettings settings) {
      throw new RuntimeException();
    }
  }

  private static class SemiJoinNonCoveringIndexPlanGenerator implements IndexPlanGenerator {
    private final SemiJoinIndexPlanCallContext context;

    private SemiJoinNonCoveringIndexPlanGenerator(SemiJoinIndexPlanCallContext context) {
      this.context = context;
    }

    @Override
    public AbstractIndexPlanGenerator getCoveringIndexGen(FunctionalIndexInfo functionInfo,
                                                          IndexGroupScan indexGroupScan,
                                                          RexNode indexCondition,
                                                          RexNode remainderCondition,
                                                          RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinToRowKeyJoinGenerator(context, functionInfo, indexGroupScan, indexCondition, remainderCondition, builder, settings);
    }

    @Override
    public AbstractIndexPlanGenerator getNonCoveringIndexGen(IndexDescriptor indexDesc,
                                                             IndexGroupScan indexGroupScan,
                                                             RexNode indexCondition,
                                                             RexNode remainderCondition,
                                                             RexNode totalCondition,
                                                             RexBuilder builder, PlannerSettings settings) {
      return new SemiJoinMergeRowKeyJoinGenerator(context, indexDesc, indexGroupScan, indexCondition, remainderCondition, totalCondition, builder, settings);
    }
  }

  private static class MatchJSPFPS extends AbstractMatchFunction<SemiJoinIndexPlanCallContext> {
    Map<String, RexCall> flattenMap = Maps.newHashMap();

    @Override
    public boolean match(RelOptRuleCall call) {
      DrillJoinRel joinRel = call.rel(0);
      DrillScanRel leftRel = call.rel(1);
      DrillAggregateRel aggRel = call.rel(2);
      DrillProjectRel upperProject = call.rel(3);
      DrillFilterRel filter = call.rel(4);
      DrillProjectRel lowerProject = call.rel(5);
      DrillScanRel rightRel = call.rel(6);


      if (!leftRel.getTable().getQualifiedName().equals(rightRel.getTable().getQualifiedName())) {
        return false;
      }

      if (aggRel.getAggCallList().size() > 0) {
        return false;
      }

      //check if the lower project contains the flatten.
      return containsFlatten(rightRel, lowerProject, flattenMap);
    }

    @Override
    public SemiJoinIndexPlanCallContext onMatch(RelOptRuleCall call) {
      final DrillScanRel leftScan = call.rel(1);

      final DrillProjectRel upperProject = call.rel(3);
      final DrillFilterRel filter = call.rel(4);
      final DrillProjectRel lowerProject = call.rel(5);
      final DrillScanRel rightScan = call.rel(6);

      final DrillJoinRel join = call.rel(0);
      final DrillAggregateRel distinct = call.rel(2);

      FlattenIndexPlanCallContext rightSideContext = new FlattenIndexPlanCallContext(call, upperProject, filter, lowerProject,null, rightScan, flattenMap);
      IndexLogicalPlanCallContext leftSideContext = new IndexLogicalPlanCallContext(call, null, null, null, leftScan);
      SemiJoinIndexPlanCallContext idxContext = new SemiJoinIndexPlanCallContext(call, join, distinct, leftSideContext, rightSideContext);
      return idxContext;
    }
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return match.match(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    doOnMatch(match.onMatch(call));
  }

  private void doOnMatch(SemiJoinIndexPlanCallContext indexContext) {
    if (indexContext.join != null) {
      FlattenIndexPlanCallContext context = transformJoinToSingleTableScan(indexContext);
      indexContext.set(context);
      if (FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.doOnMatch(context, new SemiJoinCoveringIndexPlanGenerator(indexContext))){
        return;
      }
    }
    //As we couldn't generate a single table scan for the join,
    //try index planning by only considering the PFPS.
    FlattenToIndexScanPrule.FILTER_PROJECT_SCAN.doOnMatch(indexContext.rightSide, new SemiJoinNonCoveringIndexPlanGenerator(indexContext));
  }

  private FlattenIndexPlanCallContext transformJoinToSingleTableScan(SemiJoinIndexPlanCallContext context) {
    DrillScanRel newScan = constructScan(context);
    DrillProjectRel projectRel = constructProject(context.join.getCluster(), newScan, context.leftSide.scan,
            context.rightSide.scan, context.rightSide.lowerProject.getTraitSet(),
            context.rightSide.lowerProject);
    DrillFilterRel filterRel = constructFilter(context, projectRel, true);
    DrillProjectRel upperProjectRel = constructProject(context.join.getCluster(), filterRel, context.leftSide.scan,
            context.rightSide.scan, context.rightSide.upperProject.getTraitSet(),
            context.rightSide.upperProject);
    Map<String, RexCall> flattenMap = Maps.newHashMap();
    AbstractMatchFunction.containsFlatten(newScan, projectRel, flattenMap);

    FlattenIndexPlanCallContext logicalPlanCallContext = new FlattenIndexPlanCallContext(context.call, upperProjectRel, filterRel,
            projectRel, null, newScan, flattenMap);
    return logicalPlanCallContext;
  }

  private DrillFilterRel constructFilter(SemiJoinIndexPlanCallContext context,
                                         DrillProjectRel project, boolean convertCondition) {
    RexNode condition = context.rightSide.filter.getCondition();
    if (convertCondition) {
      condition = transform(context.join.getCluster().getRexBuilder(), condition, context.rightSide.lowerProject.getRowType(), project.getRowType());
    }
    return new DrillFilterRel(context.rightSide.filter.getCluster(),
            context.rightSide.filter.getTraitSet(),
            project, condition);
  }

  private DrillProjectRel constructProject(RelOptCluster cluster, DrillRel newScan, DrillRel otherScan, DrillRel oldScan,
                                           RelTraitSet traits, DrillProjectRel oldProject) {
    List<RexNode> leftProjects = projects(newScan, otherScan.getRowType().getFieldNames());
    List<RexNode> rightProjects = projectsTransformer(cluster.getRexBuilder(), oldProject.getProjects(), oldScan.getRowType(), newScan.getRowType());
    List<String> leftProjectsNames = otherScan.getRowType().getFieldNames();
    List<String> rightProjectsNames = oldProject.getRowType().getFieldNames();
    List<RelDataType> projectTypes = relDataTypes(ListUtils.union(rightProjects, leftProjects));
    RelDataType projectRowType = cluster.getTypeFactory()
            .createStructType(projectTypes,
                    ListUtils.union(rightProjectsNames, leftProjectsNames));
    DrillProjectRel projectRel = DrillProjectRel.create(cluster, traits,
            newScan, ListUtils.union(rightProjects, leftProjects),
            projectRowType);
    return projectRel;
  }

  private List<RexNode> projectsTransformer(RexBuilder builder, List<RexNode> projects,
                                            RelDataType oldRowType, RelDataType newRowType) {
    List<RexNode> result = Lists.newArrayList();
    DrillRelOptUtil.RexFieldsTransformer transformer = new DrillRelOptUtil.RexFieldsTransformer(builder, oldRowType, newRowType);
    for (RexNode expr : projects) {
      result.add(transformer.go(expr));
    }
    return result;
  }

  private RexNode transform(RexBuilder builder, RexNode rexNode,
                            RelDataType oldRowType, RelDataType newRowType) {
    DrillRelOptUtil.RexFieldsTransformer transformer = new DrillRelOptUtil.RexFieldsTransformer(builder, oldRowType, newRowType);
    return transformer.go(rexNode);
  }

  private List<RelDataType> relDataTypes(List<RexNode> projects) {
    List<RelDataType> types = Lists.newArrayList();
    for (RexNode project : projects) {
      types.add(project.getType());
    }
    return types;
  }

  private List<RexNode> projects(DrillRel scan, List<String> names) {
    List<RexNode> projects = Lists.newArrayList();
    List<String> scanFields = scan.getRowType().getFieldNames();
    List<RelDataTypeField> scanFieldTypes = scan.getRowType().getFieldList();
    for (String name : names) {
      int fieldIndex = scanFields.indexOf(name);
      projects.add(RexInputRef.of(fieldIndex, scanFieldTypes));
    }
    return projects;
  }

  private DrillScanRel constructScan(SemiJoinIndexPlanCallContext context) {

    List<String> rightSideColumns = context.rightSide.scan.getRowType().getFieldNames();
    List<String> leftSideColumns = context.leftSide.scan.getRowType().getFieldNames();
    List<RelDataType> rightSideTypes = relDataTypeFromRelFieldType(context.rightSide.scan.getRowType().getFieldList());
    List<RelDataType> leftSideTypes = relDataTypeFromRelFieldType(context.leftSide.scan.getRowType().getFieldList());

    return new DrillScanRel(context.join.getCluster(),
            context.rightSide.scan.getTraitSet(),
            context.rightSide.scan.getTable(),
            context.join.getCluster().getTypeFactory().createStructType(ListUtils.union(leftSideTypes, rightSideTypes),
                    ListUtils.union(leftSideColumns, rightSideColumns)),
            context.rightSide.scan.getColumns(), false);
  }

  private List<RelDataType> relDataTypeFromRelFieldType(List<RelDataTypeField> fieldTypes) {
    List<RelDataType> result = Lists.newArrayList();

    for (RelDataTypeField type : fieldTypes) {
      result.add(type.getType());
    }
    return result;
  }
}
