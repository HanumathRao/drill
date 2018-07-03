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
package org.apache.drill.exec.planner.physical.visitor;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.google.common.base.Preconditions;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.physical.JoinPrel;
import org.apache.drill.exec.planner.physical.LateralJoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.calcite.rel.RelNode;

import com.google.common.collect.Lists;
import org.apache.drill.exec.planner.physical.UnnestPrel;

public class JoinPrelRenameVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{

  private final Map<String, Prel> sourceOperatorRegistry = new HashMap();

  private static JoinPrelRenameVisitor INSTANCE = new JoinPrelRenameVisitor();

  public static Prel insertRenameProject(Prel prel){
    return prel.accept(INSTANCE, null);
  }

  private void register(Prel toRegister) {
    this.sourceOperatorRegistry.put(toRegister.getClass().getSimpleName(), toRegister);
  }

  private Prel getRegisteredPrel(Class<?> classname) {
    return this.sourceOperatorRegistry.get(classname.getSimpleName());
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    return preparePrel(prel, getChildren(prel, -1));
  }

  public void unRegister(Prel unregister) {
    this.sourceOperatorRegistry.remove(unregister.getClass().getSimpleName());
  }

  private List<RelNode> getChildren(Prel prel, int registerForChild) {
    int i=0;
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      if (registerForChild == i) {
        register(prel);
      }
      child = child.accept(this, null);
      if (registerForChild == i) {
        unRegister(prel);
      }
      children.add(child);
      i++;
    }
    return children;
  }

  private Prel preparePrel(Prel prel, List<RelNode> renamedNodes) {
    return (Prel) prel.copy(prel.getTraitSet(), renamedNodes);
  }

  @Override
  public Prel visitJoin(JoinPrel prel, Void value) throws RuntimeException {

    List<RelNode> children = getChildren(prel, -1);

    final int leftCount = children.get(0).getRowType().getFieldCount();

    List<RelNode> reNamedChildren = Lists.newArrayList();

    RelNode left = prel.getJoinInput(0, children.get(0));
    RelNode right = prel.getJoinInput(leftCount, children.get(1));

    reNamedChildren.add(left);
    reNamedChildren.add(right);

    return preparePrel(prel, reNamedChildren);
  }

  @Override
  public Prel visitLateral(LateralJoinPrel prel, Void value) throws RuntimeException {

    List<RelNode> children = getChildren(prel, 1);

    final int leftCount = prel.getInputSize(0,children.get(0));

    List<RelNode> reNamedChildren = Lists.newArrayList();

    RelNode left = prel.getLateralInput(0, children.get(0));
    RelNode right = prel.getLateralInput(leftCount, children.get(1));

    reNamedChildren.add(left);
    reNamedChildren.add(right);

    return preparePrel(prel, reNamedChildren);
  }

  @Override
  public Prel visitUnnest(UnnestPrel prel, Void value) throws RuntimeException {
    Preconditions.checkArgument(getRegisteredPrel(prel.getParentClass()) instanceof LateralJoinPrel);

    LateralJoinPrel lateralJoinPrel = (LateralJoinPrel) getRegisteredPrel(prel.getParentClass());
    int correlationIndex = lateralJoinPrel.getRequiredColumns().nextSetBit(0);
    String correlationColumnName = lateralJoinPrel.getLeft().getRowType().getFieldNames().get(correlationIndex);
    RexBuilder builder = prel.getCluster().getRexBuilder();
    RexNode corrRef = builder.makeCorrel(lateralJoinPrel.getLeft().getRowType(), lateralJoinPrel.getCorrelationId());
    RexNode fieldAccess = builder.makeFieldAccess(corrRef, correlationColumnName, false);
    UnnestPrel unnestPrel = new UnnestPrel(prel.getCluster(), prel.getTraitSet(), prel.getRowType(), fieldAccess);
    return unnestPrel;
  }
}
