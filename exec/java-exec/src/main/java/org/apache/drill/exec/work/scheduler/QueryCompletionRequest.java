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

public class QueryCompletionRequest extends Request {
  private final QueueContext queueContext;
  private final DrillbitContext context;

  public QueryCompletionRequest(QueueContext queueContext,
                                DrillbitContext context,
                                DrillbitEndpoint endpoint,
                                QueryId queryId, Integer queueId) {
    super(context, endpoint, queryId, queueId);
    this.queueContext = queueContext;
    this.context = context;
  }

  public void process() {
    Request request;
    synchronized (queueContext) {
      queueContext.finishRunningQuery();
      request = queueContext.getWaitingRequest();
      if (request != null) {
        queueContext.offerInRunningQueue();
      }
    }

    if (request != null) {
      request.sendResponse(QueryStatus.ADMIT_QUERY, context.getEndpoint());
    }
  }
}
