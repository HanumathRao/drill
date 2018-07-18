package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.physical.LateralJoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.SortPrel;
import org.apache.drill.exec.planner.physical.UnnestPrel;

import java.util.List;

public class LateralUnnestRowIDVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{

  private static LateralUnnestRowIDVisitor INSTANCE = new LateralUnnestRowIDVisitor();

  public static Prel insertRowID(Prel prel){
    return prel.accept(INSTANCE, null);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    return (Prel) prel.copy(prel.getTraitSet(), getChildren(prel));
  }

  private List<RelNode> getChildren(Prel prel) {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }
    return children;
  }

  @Override
  public Prel visitSort(SortPrel prel, Void value) throws RuntimeException {
    return null;
  }

  @Override
  public Prel visitLateral(LateralJoinPrel prel, Void value) throws RuntimeException {
    return null;
  }

  @Override
  public Prel visitUnnest(UnnestPrel prel, Void value) throws RuntimeException {
    return null;
  }

  @Override
  public Prel visitProject(ProjectPrel prel, Void value) throws RuntimeException {
    return null;
  }

  @Override
  public Prel visit
}
