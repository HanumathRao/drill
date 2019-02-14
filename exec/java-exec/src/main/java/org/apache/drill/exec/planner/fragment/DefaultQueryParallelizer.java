package org.apache.drill.exec.planner.fragment;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.Collection;
import java.util.Set;

public class DefaultQueryParallelizer extends SimpleParallelizer {

  public DefaultQueryParallelizer(QueryContext queryContext) {
    super(queryContext);
  }

  public DefaultQueryParallelizer(long parallelizationThreshold, int maxWidthPerNode, int maxGlobalWidth, double affinityFactor) {
    super(parallelizationThreshold, maxWidthPerNode, maxGlobalWidth, affinityFactor);
  }

  protected void adjustMemory(PlanningSet planningSet, Set<Wrapper> roots,
                              Collection<CoordinationProtos.DrillbitEndpoint> activeEndpoints) {

  }
}
