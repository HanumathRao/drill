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
    //queryrm.selectQueue( pass the max node Resource) returns queue configuration.


  }
}