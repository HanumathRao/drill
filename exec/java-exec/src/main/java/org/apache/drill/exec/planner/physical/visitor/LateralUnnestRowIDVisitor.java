package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.physical.LateralJoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.UnnestPrel;

import java.util.List;

public class LateralUnnestRowIDVisitor extends BasePrelVisitor<Prel, Boolean, RuntimeException> {

  private static LateralUnnestRowIDVisitor INSTANCE = new LateralUnnestRowIDVisitor();

  public static Prel insertRowID(Prel prel){
    return prel.accept(INSTANCE, false);
  }

  @Override
  public Prel visitPrel(Prel prel, Boolean isRightOfLateral) throws RuntimeException {
    List<RelNode> children = getChildren(prel, isRightOfLateral);
    if (isRightOfLateral) {
      return prel.addImplicitRowIDCol(children);
    } else {
      return (Prel) prel.copy(prel.getTraitSet(), children);
    }
  }

  private List<RelNode> getChildren(Prel prel, Boolean isRightOfLateral) {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, isRightOfLateral);
      children.add(child);
    }
    return children;
  }

  @Override
  public Prel visitLateral(LateralJoinPrel prel, Boolean value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    children.add(((Prel)prel.getInput(0)).accept(this, false));
    children.add(((Prel) prel.getInput(1)).accept(this, true));

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitUnnest(UnnestPrel prel, Boolean value) throws RuntimeException {
    return prel.addImplicitRowIDCol(null);
  }
}
