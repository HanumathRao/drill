package org.apache.drill.exec.planner.fragment;

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

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.AbstractOpWrapperVisitor;
import org.apache.drill.exec.planner.cost.NodeResource;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MemoryCalculator extends AbstractOpWrapperVisitor<Void, RuntimeException> {

  private final PlanningSet planningSet;
  private final Map<DrillbitEndpoint, List<PhysicalOperator>> bufferedOperators;
  private final QueryContext queryContext;

  public MemoryCalculator(PlanningSet planningSet, QueryContext context) {
    this.planningSet = planningSet;
    this.bufferedOperators = new HashMap<>();
    this.queryContext = context;
  }

  private Map<DrillbitEndpoint, Integer> getMinorFragCountPerDrillbit(Wrapper currFragment) {
      return currFragment.getAssignedEndpoints().stream()
                                                .collect(Collectors.groupingBy(Function.identity(),
                                                                               Collectors.summingInt(x -> 1)));
  }

  private void merge(Wrapper currFrag, Map<DrillbitEndpoint, Integer> minorFragsPerDrillBit, Function<Integer, Integer> getMemory) {
    NodeResource.merge(currFrag.getResourceMap(),
                       minorFragsPerDrillBit.entrySet().stream().collect(Collectors.toMap((Map.Entry<DrillbitEndpoint, Integer> x) -> x.getKey(),
                                                                                          (Map.Entry<DrillbitEndpoint, Integer> x) -> NodeResource.create(0,
                                                                                                                                          getMemory.apply(x.getValue())))));
  }

  @Override
  public Void visitSendingExchange(Exchange exchange, Wrapper fragment) throws RuntimeException {
    Wrapper receivingFragment = planningSet.get(fragment.getNode().getSendingExchangePair().getNode());
    merge(fragment, getMinorFragCountPerDrillbit(fragment), (x) -> exchange.getSenderMemory(receivingFragment.getWidth(), x));
    return visitOp(exchange, fragment);
  }

  @Override
  public Void visitReceivingExchange(Exchange exchange, Wrapper fragment) throws RuntimeException {
    Wrapper receivingFragment = planningSet.get(fragment.getNode().getSendingExchangePair().getNode());
    merge(fragment, getMinorFragCountPerDrillbit(fragment), (x) -> exchange.getReceiverMemory(receivingFragment.getWidth(), x));
    return null;
  }

  public List<PhysicalOperator> getBufferedOperators(DrillbitEndpoint endpoint) {
    return this.bufferedOperators.getOrDefault(endpoint, new ArrayList<>());
  }

  @Override
  public Void visitOp(PhysicalOperator op, Wrapper fragment) {
    int memoryCost = (int)Math.ceil(op.getCost().getMemoryCost());
    if (op.isBufferedOperator(queryContext)) {
      int memoryCostPerMinorFrag = (int)Math.ceil(memoryCost/fragment.getWidth());
      merge(fragment, getMinorFragCountPerDrillbit(fragment), (x) -> memoryCostPerMinorFrag * x);
    } else {
      merge(fragment, getMinorFragCountPerDrillbit(fragment), (x) -> memoryCost * x);
    }
    for (PhysicalOperator child : op) {
      child.accept(this, fragment);
    }
    return null;
  }
}

