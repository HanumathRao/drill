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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.ListUtils;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.drill.exec.planner.index.SemiJoinIndexPlanCallContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.physical.HashAggPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.RowKeyJoinPrel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.Prule;
import static org.apache.drill.exec.planner.physical.AggPrelBase.OperatorPhase.PHASE_1of1;
import static org.apache.drill.exec.planner.physical.Prel.DRILL_PHYSICAL;
import java.util.List;

/**
 * Generate a Non covering index plan that is semantically equivalent to the original plan.
 *
 * This plan will be further optimized by the filter pushdown rule of the Index plugin which should
 * push this filter into the index scan.
 */
public class SemiJoinMergeRowKeyJoinGenerator extends NonCoveringIndexPlanGenerator {
  private final SemiJoinIndexPlanCallContext joinContext;
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
    this.joinContext = indexContext;
  }

  @Override
  public boolean forceConvert() {
    return true;
  }

  @Override
  public RelNode convertChild(final RelNode join, final RelNode input) throws InvalidRelException {
    List<ProjectPrel> projectRels = Lists.newArrayList();
    RelNode nonCoveringIndexScanPrel = super.convertChild(join, input);
    RowKeyJoinPrel rkj = getRowKeyJoin(nonCoveringIndexScanPrel, projectRels);
    List<RelNode> joinsRightProjectRels = splitRowKeyJoin(rkj, projectRels);
    RelNode aggInput = getRightInputOfRowKeyJoin(rkj, projectRels);
    HashAggPrel agg = buildHashAgg(joinContext.distinct, aggInput);
    List<RelNode> leftSideRelsOfInJoin = getleftSideRelsOfJoin(joinContext);
    List<RelNode> rkjLeftRels = merge(joinsRightProjectRels, leftSideRelsOfInJoin);
    rkjLeftRels.add(buildProject(rkjLeftRels.get(rkjLeftRels.size() - 1), leftSideRelsOfInJoin.get(leftSideRelsOfInJoin.size()-1)));
    return buildRowKeyJoin(rkjLeftRels.get(rkjLeftRels.size()-1),agg);
  }

  private RelNode buildRowKeyJoin(RelNode leftInput, RelNode distinct ) throws InvalidRelException {
    return new RowKeyJoinPrel(leftInput.getCluster(), leftInput.getTraitSet(), leftInput, distinct,
                              joinContext.join.getCondition(), JoinRelType.INNER);
  }

  private List<RelNode> merge(List<RelNode> leftSideRels, List<RelNode> rightSideRels) {
    List<RelNode> leftRels = Lists.reverse(leftSideRels);
    List<RelNode> rightRels = rightSideRels;

    int largest = Math.max(leftRels.size(), rightRels.size());
    List<RelNode> result = Lists.newArrayList();
    RelNode lastValidLeftInput = null;
    RelNode lastValidRightInput = null;
    for (int i=0;i<largest;i++) {
      lastValidLeftInput = lastValidLeftInput == null ? leftRels.get(0) : i - 1 < leftRels.size() ? leftRels.get(i-1) : lastValidLeftInput;
      lastValidRightInput = lastValidRightInput == null ? rightRels.get(0) : i - 1 < rightRels.size() ? rightRels.get(i-1) : lastValidRightInput;
      RelNode leftRel = i < leftRels.size() ? leftRels.get(i) : buildProject(lastValidLeftInput, lastValidLeftInput);
      RelNode rightRel = i < rightRels.size() ? rightRels.get(i) : buildProject(lastValidRightInput, lastValidRightInput);
      result.addAll(merge(leftRel, rightRel, result.size() == 0 ? null : result.get(result.size()-1)));
    }
    return result;
  }

  private ProjectPrel buildProject(RelNode input, RelNode projectsRelNode) {
    return new ProjectPrel(input.getCluster(), input.getTraitSet(), input,
            IndexPlanUtils.projects(input, projectsRelNode.getRowType().getFieldNames()),projectsRelNode.getRowType());
  }

  private ProjectPrel buildProject(RelNode input, List<RexNode> projects, RelDataType outputType) {
    return new ProjectPrel(input.getCluster(), input.getTraitSet(), input,projects, outputType);
  }

  private List<RelNode> merge(RelNode leftRel, RelNode rightRel, RelNode input) {
    List<RelNode> result = Lists.newArrayList();
    if (input == null) {
      result.add(merge((ScanPrel) leftRel, (ScanPrel) rightRel));
      return result;
    }
    if (leftRel instanceof ProjectPrel && rightRel instanceof ProjectPrel) {
      result.add(merge((ProjectPrel)leftRel, (ProjectPrel)rightRel, input));
    } else if (leftRel instanceof FilterPrel && rightRel instanceof FilterPrel) {
      result.add(merge((FilterPrel) leftRel, (FilterPrel) rightRel, input));
    } else if (leftRel instanceof  ProjectPrel ){
      result.addAll(mergeFilterProjectRels((ProjectPrel)leftRel, (FilterPrel)rightRel, input));
    } else {
      result.addAll(mergeFilterProjectRels((ProjectPrel) rightRel, (FilterPrel)leftRel, input));
    }
    return result;
  }

  private ScanPrel merge(ScanPrel leftScan, ScanPrel rightScan) {

    List<String> rightSideColumns = rightScan.getRowType().getFieldNames();
    List<String> leftSideColumns = leftScan.getRowType().getFieldNames();
    List<RelDataType> rightSideTypes = relDataTypeFromRelFieldType(rightScan.getRowType().getFieldList());
    List<RelDataType> leftSideTypes = relDataTypeFromRelFieldType(leftScan.getRowType().getFieldList());
    DbGroupScan restrictedGroupScan  = ((DbGroupScan) IndexPlanUtils.getGroupScan(leftScan))
                                              .getRestrictedScan(ListUtils.union(leftSideColumns,rightSideColumns));
    return new ScanPrel(leftScan.getCluster(),
            rightScan.getTraitSet(),
            restrictedGroupScan,
            leftScan.getCluster().getTypeFactory().createStructType(ListUtils.union(leftSideTypes, rightSideTypes),
                    ListUtils.union(leftSideColumns, rightSideColumns)),
            leftScan.getTable());
  }

  private RelNode merge(FilterPrel filterL, FilterPrel filterR, RelNode input) {
    List<RexNode> combineConditions = Lists.newArrayList();
    combineConditions.add(filterL.getCondition());
    combineConditions.add(filterR.getCondition());

    return new FilterPrel(input.getCluster(), input.getTraitSet(), input,
            RexUtil.composeConjunction(input.getCluster().getRexBuilder(), combineConditions, false));
  }

  private RelNode merge(ProjectPrel projectL, ProjectPrel projectR, RelNode input) {
    RexBuilder builder = input.getCluster().getRexBuilder();
    List<RexNode> combinedProjects = Lists.newArrayList(); //Sets.newHashSet();
    combinedProjects.addAll(IndexPlanUtils.projectsTransformer(builder, projectL.getProjects(),
                                        projectL.getInput().getRowType(), input.getRowType()));
    combinedProjects.addAll(IndexPlanUtils.projectsTransformer(builder, projectR.getProjects(),
                                        projectR.getInput().getRowType(), input.getRowType()));
    List<RexNode> listOfProjects = Lists.newArrayList();
    listOfProjects.addAll(combinedProjects);
    return new ProjectPrel(input.getCluster(), input.getTraitSet(), input, listOfProjects,
           merge(projectL.getRowType(), projectR.getRowType()));
  }

  private RelDataType merge(RelDataType first, RelDataType second) {
    return new RelRecordType(ListUtils.union(first.getFieldList(), second.getFieldList()));
  }

  private List<RelNode> mergeFilterProjectRels(ProjectPrel lRel, FilterPrel pRel, RelNode input) {
    List<RelNode> result = Lists.newArrayList();

    List<RexNode> updatedProjects = IndexPlanUtils.projectsTransformer(builder, lRel.getProjects(), lRel.getInput().getRowType(), input.getRowType());
    RelNode project = buildProject(input, ListUtils.union(updatedProjects,
                                      IndexPlanUtils.projects(input, pRel.getRowType().getFieldNames())),
                                  merge(lRel.getRowType(), pRel.getRowType()));
    result.add(project);
    RexNode condition = IndexPlanUtils.transform(builder, pRel.getCondition(),
                                                  pRel.getInput().getRowType(), project.getRowType());
    input = new FilterPrel(input.getCluster(), input.getTraitSet(), project, condition);
    result.add(input);
    return result;
  }

  private List<RelNode> getleftSideRelsOfJoin(SemiJoinIndexPlanCallContext joinContext) {
    List<RelNode> result = Lists.newArrayList();
    return physicalRel(joinContext.leftSide.upperProject,
                physicalRel(joinContext.leftSide.filter,
                      physicalRel(joinContext.leftSide.lowerProject,
                            physicalRel(joinContext.leftSide.scan, result))));
  }

  private List<RelNode> physicalRel(DrillScanRel scan, List<RelNode> collectionOfRels) {
    Preconditions.checkNotNull(scan);
    collectionOfRels.add(new ScanPrel(scan.getCluster(), scan.getTraitSet().plus(DRILL_PHYSICAL),
            scan.getGroupScan(), scan.getRowType(), scan.getTable()));
    return collectionOfRels;
  }

  private List<RelNode> physicalRel(DrillProjectRel projectRel, List<RelNode> collectionOfRels) {
    if (projectRel == null) {
      return collectionOfRels;
    }

    collectionOfRels.add(new ProjectPrel(projectRel.getCluster(), projectRel.getTraitSet().plus(DRILL_PHYSICAL),
                                         collectionOfRels.get(collectionOfRels.size()-1), projectRel.getProjects(), projectRel.getRowType()));
    return collectionOfRels;
  }

  private List<RelNode> physicalRel(DrillFilterRel filter, List<RelNode> collectionOfRels) {
    if (filter == null) {
      return collectionOfRels;
    }

    collectionOfRels.add(new FilterPrel(filter.getCluster(), filter.getTraitSet().plus(DRILL_PHYSICAL),
                                        collectionOfRels.get(collectionOfRels.size()-1), filter.getCondition()));
    return collectionOfRels;
  }

  private HashAggPrel buildHashAgg(DrillAggregateRel distinct, RelNode input) throws InvalidRelException {
    return new HashAggPrel(distinct.getCluster(), input.getTraitSet().plus(DRILL_PHYSICAL),input,
            distinct.indicator,distinct.getGroupSet(), distinct.getGroupSets(), distinct.getAggCallList(),PHASE_1of1);
  }

  private RowKeyJoinPrel getRowKeyJoin(RelNode node, List<ProjectPrel> projectRels) {
    Preconditions.checkArgument(node instanceof RowKeyJoinPrel ||
                                node instanceof ProjectPrel);
    if (node instanceof  ProjectPrel ) {
      projectRels.add((ProjectPrel) node);
      return getRowKeyJoin(((ProjectPrel) node).getInput(), projectRels);
    } else {
      return (RowKeyJoinPrel) node;
    }
  }

  private List<RelNode> splitRowKeyJoin(RowKeyJoinPrel rkj, List<ProjectPrel> projects) {
    List<RelNode> result = Lists.newArrayList();
    RelNode input = rkj;
    for (int last = projects.size() -1; last >=0; last--) {
      result.add(new ProjectPrel(rkj.getCluster(), projects.get(last).getTraitSet(), input,
                                 projects.get(last).getProjects(), projects.get(last).getRowType()));
      input = projects.get(last);
    }
    result = Lists.reverse(result);
    return getRelNodes(rkj.getInput(0), result);
  }

  private List<RelNode> getRelNodes(RelNode node, List<RelNode> result) {
    if (node.getInputs().size() == 0) {
      result.add(node);
      return result;
    }

    result.add(node);
    return getRelNodes(IndexPlanUtils.sole(node.getInputs()), result);
  }

  private RelNode getRightInputOfRowKeyJoin(RowKeyJoinPrel rkj, List<ProjectPrel> projects) {
    return rkj.getInput(1);
//    RelNode input = rkj.getInput(1);
//    for (int i = 0; i < projects.size(); i++) {
//      input = new ProjectPrel(rkj.getCluster(), projects.get(i).getTraitSet(), input,
//              projects.get(i).getProjects(), projects.get(i).getRowType());
//    }
//    return input;
  }

  @Override
  public boolean go() throws InvalidRelException {
    RelNode top = indexContext.getCall().rel(0);
    if (top instanceof DrillJoinRel) {
      DrillJoinRel join = (DrillJoinRel) top;
      final RelNode input0 = join.getInput(0);
      final RelNode input1 = join.getInput(1);
      RelTraitSet traits0 = input0.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput0 = Prule.convert(input0, traits0);
      RelTraitSet traits1 = input1.getTraitSet().plus(DRILL_PHYSICAL);
      RelNode convertedInput1 = Prule.convert(input1, traits1);
      return this.go(top, convertedInput0) || this.go(top, convertedInput1);
    } else {
      return false;
    }
  }

  private List<RelDataType> relDataTypeFromRelFieldType(List<RelDataTypeField> fieldTypes) {
    List<RelDataType> result = Lists.newArrayList();

    for (RelDataTypeField type : fieldTypes) {
      result.add(type.getType());
    }
    return result;
  }
}
