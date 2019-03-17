package org.apache.drill.exec.planner.fragment;

import org.apache.drill.common.util.function.CheckedConsumer;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.AbstractOpWrapperVisitor;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.util.memory.DefaultRMMemoryAllocationUtilities;
import org.apache.drill.exec.work.foreman.rm.QueryResourceManager;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.function.BiFunction;

public class DefaultQueueQueryParallelizer extends SimpleParallelizer {

  private final boolean planHasMemory;
  private final QueryContext queryContext;
  private Map<String, Collection<PhysicalOperator>> endpointMap;
  private final QueryResourceManager resourceManager;

  public DefaultQueueQueryParallelizer(boolean memoryAvailableInPlan, QueryResourceManager rm, QueryContext queryContext) {
    super(queryContext);
    this.planHasMemory = memoryAvailableInPlan;
    this.queryContext = queryContext;
    this.resourceManager = rm;
  }

  @Override
  public void adjustMemory(PlanningSet planningSet, Set<Wrapper> roots,
                           Collection<DrillbitEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
    if (planHasMemory) {
      return;
    }

    Collector collector = new Collector();

    for (Wrapper wrapper : roots) {
      traverse(wrapper, CheckedConsumer.throwingConsumerWrapper((Wrapper fragment) -> {
        fragment.getNode().getRoot().accept(collector, fragment);
      }));
    }

    endpointMap = collector.getNodeMap();

    DefaultRMMemoryAllocationUtilities.planMemory(queryContext, this.resourceManager, endpointMap);
  }


  public class Collector extends AbstractOpWrapperVisitor<Void, RuntimeException> {
    private final Multimap<DrillbitEndpoint, PhysicalOperator> bufferedOperators;

    public Collector() {
      this.bufferedOperators = ArrayListMultimap.create();
    }

    private void getMinorFragCountPerDrillbit(Wrapper currFragment, PhysicalOperator operator) {
      for (DrillbitEndpoint endpoint : currFragment.getAssignedEndpoints()) {
        bufferedOperators.put(endpoint, operator);
      }
    }

    @Override
    public Void visitSendingExchange(Exchange exchange, Wrapper fragment) throws RuntimeException {
      return visitOp(exchange, fragment);
    }

    @Override
    public Void visitReceivingExchange(Exchange exchange, Wrapper fragment) throws RuntimeException {
      return null;
    }

    @Override
    public Void visitOp(PhysicalOperator op, Wrapper fragment) {
      if (op.isBufferedOperator(queryContext)) {
        getMinorFragCountPerDrillbit(fragment, op);
      }
      for (PhysicalOperator child : op) {
        child.accept(this, fragment);
      }
      return null;
    }

    public Map<String, Collection<PhysicalOperator>> getNodeMap() {
      Map<DrillbitEndpoint, Collection<PhysicalOperator>> endpointCollectionMap = bufferedOperators.asMap();
      Map<String, Collection<PhysicalOperator>> nodeMap = new HashMap<>();
      for (Map.Entry<DrillbitEndpoint, Collection<PhysicalOperator>> entry : endpointCollectionMap.entrySet()) {
        nodeMap.put(entry.getKey().getAddress(), entry.getValue());
      }

      return nodeMap;
    }
  }

  @Override
  protected BiFunction<DrillbitEndpoint, PhysicalOperator, Long> getMemory() {
    return (endpoint, operator) -> operator.getMaxAllocation();
  }
}
