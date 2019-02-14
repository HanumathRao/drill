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
package org.apache.drill.exec.planner.cost;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import java.util.Map;

public class NodeResource {
  private int cpu;
  private int memory;

  public NodeResource(int cpu, int memory) {
    this.cpu = cpu;
    this.memory = memory;
  }

  public void setMemory(int memory) {
    this.memory = memory;
  }

  public void setCpu(int cpu) {
    this.cpu = cpu;
  }

  public void add(NodeResource other) {
    if (other == null) {
      return;
    }
    this.cpu += other.cpu;
    this.memory += other.memory;
  }

  public static Map<DrillbitEndpoint, NodeResource> merge(Map<DrillbitEndpoint, NodeResource> to,
                                                          Map<DrillbitEndpoint, NodeResource> from) {
    to.entrySet().stream().forEach((toEntry) -> toEntry.getValue().add(from.get(toEntry.getKey())));
    return to;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CPU: ").append(cpu).append("Memory: ").append(memory);
    return sb.toString();
  }

  public static NodeResource create() {
    return create(0,0);
  }

  public static NodeResource create(int cpu) {
    return create(cpu,0);
  }

  public static NodeResource create(int cpu, int memory) {
    return new NodeResource(cpu, memory);
  }
}
