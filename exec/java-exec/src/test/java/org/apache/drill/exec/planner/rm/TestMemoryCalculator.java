package org.apache.drill.exec.planner.rm;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.PlannerTest;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.cost.NodeResource;
import org.apache.drill.exec.planner.fragment.DistributionAffinity;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MemoryCalculator;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.QueuedQueryParallelizer;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.Stats;
import org.apache.drill.exec.planner.fragment.Wrapper;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import static java.lang.Integer.MAX_VALUE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(PlannerTest.class)
public class TestMemoryCalculator extends PlanTestBase {
  private static final UserSession session = UserSession.Builder.newBuilder()
    .withCredentials(UserBitShared.UserCredentials.newBuilder()
      .setUserName("foo")
      .build())
    .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
    .withOptionManager(bits[0].getContext().getOptionManager())
    .build();
  private static final CoordinationProtos.DrillbitEndpoint N1_EP1 = newDrillbitEndpoint("node1", 30010);

  private static final CoordinationProtos.DrillbitEndpoint newDrillbitEndpoint(String address, int port) {
    return CoordinationProtos.DrillbitEndpoint.newBuilder().setAddress(address).setControlPort(port).build();
  }

  private final Wrapper newWrapper(Fragment rootFragment, Map<CoordinationProtos.DrillbitEndpoint, NodeResource> resourceMap, List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    final Wrapper fragmentWrapper = mock(Wrapper.class);

    when(fragmentWrapper.getAssignedEndpoints()).thenReturn(endpoints);
    when(fragmentWrapper.getNode()).thenReturn(rootFragment);
    when(fragmentWrapper.getResourceMap()).thenReturn(resourceMap);
    when(fragmentWrapper.getWidth()).thenReturn(endpoints.size());
    return fragmentWrapper;
  }

  @Test
  public void TestProjectAndScan() throws Exception {
    List<CoordinationProtos.DrillbitEndpoint> activeEndpoints = new ArrayList<>();
    activeEndpoints.add(N1_EP1);
    String plan = getPlanInString("EXPLAIN PLAN FOR SELECT * from cp.`employee.json`", JSON_FORMAT);
    final DrillbitContext drillbitContext = getDrillbitContext();
    final QueryContext queryContext = new QueryContext(session, drillbitContext, UserBitShared.QueryId.getDefaultInstance());
    final PhysicalPlanReader planReader = drillbitContext.getPlanReader();
    Fragment rootFragment = PopUnitTestBase.getRootFragmentFromPlanString(planReader, plan);
    final PlanningSet planningSet = new PlanningSet();
    Map<CoordinationProtos.DrillbitEndpoint, NodeResource> resources = activeEndpoints.stream().collect(Collectors.toMap(x ->x, x -> NodeResource.create()));
    Wrapper fragmentWrapper = newWrapper(rootFragment, resources, activeEndpoints);
    SimpleParallelizer parallelizer = new QueuedQueryParallelizer(queryContext);
    parallelizer.initFragmentWrappers(rootFragment, planningSet);
    parallelizer.prepareFragmentTree(rootFragment);
    MemoryCalculator memoryCalculator = new MemoryCalculator(planningSet, queryContext);
    rootFragment.getRoot().accept(memoryCalculator, fragmentWrapper);
    System.out.println(fragmentWrapper.getResourceMap());
  }

  @Test
  public void TestGroupByProjectAndScan() throws Exception {
    List<CoordinationProtos.DrillbitEndpoint> activeEndpoints = new ArrayList<>();
    activeEndpoints.add(N1_EP1);
    String plan = getPlanInString("EXPLAIN PLAN FOR SELECT dept_id, count(*) from cp.`employee.json` group by dept_id", JSON_FORMAT);
    final DrillbitContext drillbitContext = getDrillbitContext();
    final QueryContext queryContext = new QueryContext(session, drillbitContext, UserBitShared.QueryId.getDefaultInstance());
    final PhysicalPlanReader planReader = drillbitContext.getPlanReader();
    Fragment rootFragment = PopUnitTestBase.getRootFragmentFromPlanString(planReader, plan);
    final PlanningSet planningSet = new PlanningSet();
    Map<CoordinationProtos.DrillbitEndpoint, NodeResource> resources = activeEndpoints.stream().collect(Collectors.toMap(x ->x, x -> NodeResource.create()));
    Wrapper fragmentWrapper = newWrapper(rootFragment, resources, activeEndpoints);
    SimpleParallelizer parallelizer = new QueuedQueryParallelizer(queryContext);
    parallelizer.initFragmentWrappers(rootFragment, planningSet);
    parallelizer.prepareFragmentTree(rootFragment);
    MemoryCalculator memoryCalculator = new MemoryCalculator(planningSet, queryContext);
    rootFragment.getRoot().accept(memoryCalculator, fragmentWrapper);
    System.out.println(fragmentWrapper.getResourceMap());
  }
}
