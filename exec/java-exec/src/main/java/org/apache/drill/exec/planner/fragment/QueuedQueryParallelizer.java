package org.apache.drill.exec.planner.fragment;

import org.apache.drill.common.util.function.CheckedConsumer;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.cost.NodeResource;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import java.util.Map;
import java.util.Collection;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class QueuedQueryParallelizer extends SimpleParallelizer {
  private final QueryContext queryContext;

  public QueuedQueryParallelizer(QueryContext queryContext) {
    super(queryContext);
    this.queryContext = queryContext;
  }

  protected void adjustMemory(PlanningSet planningSet, Set<Wrapper> roots,
                              final Collection<DrillbitEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
    final Map<DrillbitEndpoint, NodeResource> totalNodeResources = activeEndpoints.stream().collect(Collectors.toMap(x ->x, x -> NodeResource.create()));
    final Map<DrillbitEndpoint, List<PhysicalOperator>> operators = activeEndpoints.stream().collect(Collectors.toMap(x -> x, x -> new ArrayList<>()));

    for (Wrapper wrapper : roots) {
      traverse(wrapper, CheckedConsumer.throwingConsumerWrapper((Wrapper fragment) -> {
        MemoryCalculator calculator = new MemoryCalculator(planningSet, queryContext);
        fragment.getNode().getRoot().accept(calculator, fragment);
        NodeResource.merge(totalNodeResources, fragment.getResourceMap());
        operators.entrySet().stream().forEach((entry) -> entry.getValue().addAll(calculator.getBufferedOperators(entry.getKey())));
      }));
    }

  }
}