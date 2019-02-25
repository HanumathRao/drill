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

import org.apache.calcite.util.Pair;
import org.apache.drill.common.util.function.CheckedConsumer;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.cost.NodeResource;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class QueuedQueryParallelizer extends SimpleParallelizer {
  private final boolean enableMemoryPlanning;
  private final QueryContext queryContext;
  private final Map<DrillbitEndpoint, Map<PhysicalOperator, Long>> operators;

  public QueuedQueryParallelizer(boolean memoryPlanning, QueryContext queryContext) {
    super(queryContext);
    this.enableMemoryPlanning = memoryPlanning;
    this.queryContext = queryContext;
    this.operators = new HashMap<>();
  }

  public BiFunction<DrillbitEndpoint, PhysicalOperator, Long> getMemory() {
    return (x, y) -> operators.get(x).get(y);
  }

  public void adjustMemory(PlanningSet planningSet, Set<Wrapper> roots,
                           Collection<DrillbitEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
    final Map<DrillbitEndpoint, NodeResource> totalNodeResources =
            activeEndpoints.stream().collect(Collectors.toMap(x ->x,
                                                              x -> NodeResource.create()));
    final Map<DrillbitEndpoint, List<Pair<PhysicalOperator, Long>>> operators =
            activeEndpoints.stream().collect(Collectors.toMap(x -> x,
                                                              x -> new ArrayList<>()));

    for (Wrapper wrapper : roots) {
      traverse(wrapper, CheckedConsumer.throwingConsumerWrapper((Wrapper fragment) -> {
        MemoryCalculator calculator = new MemoryCalculator(planningSet, queryContext);
        fragment.getNode().getRoot().accept(calculator, fragment);
        NodeResource.merge(totalNodeResources, fragment.getResourceMap());
        operators.entrySet()
                  .stream()
                  .forEach((entry) -> entry.getValue()
                                           .addAll(calculator.getBufferedOperators(entry.getKey())));
      }));
    }
    //queryrm.selectQueue( pass the max node Resource) returns queue configuration.
    Map<DrillbitEndpoint, List<Pair<PhysicalOperator, Long>>> memoryAdjustedOperators = adjustMemoryForOperators(operators, totalNodeResources, 10);
    memoryAdjustedOperators.entrySet().stream().forEach((x) -> {
      Map<PhysicalOperator, Long> memoryPerOperator = x.getValue().stream()
                                                                  .collect(Collectors.toMap(operatorLongPair -> operatorLongPair.left,
                                                                                            operatorLongPair -> operatorLongPair.right));
      this.operators.put(x.getKey(), memoryPerOperator);
    });
  }


  private Map<DrillbitEndpoint, List<Pair<PhysicalOperator, Long>>>
      adjustMemoryForOperators(Map<DrillbitEndpoint, List<Pair<PhysicalOperator, Long>>> memoryPerOperator,
                               Map<DrillbitEndpoint, NodeResource> nodeResourceMap, int nodeLimit) {

    Map<DrillbitEndpoint, List<Pair<PhysicalOperator, Long>>> onlyMemoryAboveLimitOperators = new HashMap<>();
    memoryPerOperator.entrySet().stream().forEach((entry) -> {
      onlyMemoryAboveLimitOperators.putIfAbsent(entry.getKey(), new ArrayList<>());
      if (nodeResourceMap.get(entry.getKey()).getMemory() > nodeLimit) {
        onlyMemoryAboveLimitOperators.get(entry.getKey()).addAll(entry.getValue());
      }
    });

    Map<DrillbitEndpoint, List<Pair<PhysicalOperator, Long>>> memoryAdjustedDrillbits = new HashMap<>();
    onlyMemoryAboveLimitOperators.entrySet().stream().forEach(
      entry -> {
        Long totalMemory = entry.getValue().stream().map(operatorMemory -> operatorMemory.getValue()).reduce(0L, (x,y) -> x+ y);
        List<Pair<PhysicalOperator, Long>> adjustedMemory = entry.getValue().stream().map(operatorMemory -> {
          return Pair.of(operatorMemory.getKey(), (long) Math.ceil(operatorMemory.getValue()/totalMemory * nodeLimit));
        }).collect(Collectors.toList());
        memoryAdjustedDrillbits.put(entry.getKey(), adjustedMemory);
      }
    );

    Map<DrillbitEndpoint, List<Pair<PhysicalOperator, Long>>> allDrillbits = new HashMap<>();
    memoryPerOperator.entrySet().stream().filter((entry) -> !memoryAdjustedDrillbits.containsKey(entry.getKey())).forEach(
      operatorMemory -> {
        allDrillbits.put(operatorMemory.getKey(), operatorMemory.getValue());
      }
    );

    memoryAdjustedDrillbits.entrySet().stream().forEach(
      operatorMemory -> {
        allDrillbits.put(operatorMemory.getKey(), operatorMemory.getValue());
      }
    );

    return allDrillbits;
  }
}