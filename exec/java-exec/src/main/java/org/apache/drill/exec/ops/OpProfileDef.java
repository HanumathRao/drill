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
package org.apache.drill.exec.ops;

public class OpProfileDef {

  public int operatorId;
  public int operatorType;
  public int incomingCount;
  public long memory_assigned;

  public OpProfileDef(int operatorId, int operatorType, int incomingCount, long memory_assigned) {
    this.operatorId = operatorId;
    this.operatorType = operatorType;
    this.incomingCount = incomingCount;
    this.memory_assigned = memory_assigned;
  }
  public int getOperatorId(){
    return operatorId;
  }

  public int getOperatorType(){
    return operatorType;
  }
  public int getIncomingCount(){
    return incomingCount;
  }

}
