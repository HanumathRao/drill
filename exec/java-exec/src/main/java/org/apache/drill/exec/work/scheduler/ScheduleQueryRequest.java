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
package org.apache.drill.exec.work.scheduler;


import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.server.DrillbitContext;


public class ScheduleQueryRequest extends Request {
  private final int ALLOWABLE_RUNNING_LIMIT = 10;
  private final int ALLOWABLE_WAITING_LIMIT = 10;
  private final DrillbitContext context;
  private final QueueContext queueContext;

  public ScheduleQueryRequest(QueueContext queueContext,
                              DrillbitContext context,
                              DrillbitEndpoint sender,
                              QueryId queryId, Integer queueId) {
    super(context, sender, queryId, queueId);
    this.context = context;
    this.queueContext = queueContext;
  }

  public void process() {
    QueryStatus status;
    synchronized(queueContext) {
      int waiting_size = queueContext.getNumOfWaitingQueries();
      if (waiting_size >= ALLOWABLE_WAITING_LIMIT) {
        status = QueryStatus.QUEUE_BUSY;
      } else if (waiting_size > 0) {
        queueContext.offerInWaitingQueue(this);
        status = QueryStatus.WAIT_QUERY;
      }
      else {
        int running_count = queueContext.getNumOfRunningQueries();
        if (running_count > ALLOWABLE_RUNNING_LIMIT) {
          queueContext.offerInWaitingQueue(this);
          status = QueryStatus.WAIT_QUERY;
        } else {
          queueContext.offerInRunningQueue();
          status = QueryStatus.ADMIT_QUERY;
        }
      }
    }
    sendResponse(status, context.getEndpoint());
  }
}
