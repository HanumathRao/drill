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

import org.apache.drill.exec.proto.BitControl.SchedulingMessageType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.server.DrillbitContext;

public class RequestCreator {
  public static Request create(QueueContext queueContext,
                               DrillbitContext context,
                               SchedulingMessageType messageType,
                               DrillbitEndpoint sender,
                               QueryId qryID, Integer qID) {
    switch (messageType) {
      case SCHEDULE_A_QUERY :
        return new ScheduleQueryRequest(queueContext, context, sender, qryID, qID);
      case QUERY_COMPLETE:
        return new QueryCompletionRequest(queueContext, context, sender, qryID, qID);
//      case ADMIT_QUERY_TO_QUEUE:
//        return new AdmissionRequest(context, sender, qryID, qID);
//      case NOT_A_LEADER_FOR_QUEUE:
//        return new NotALeaderRequest(context, sender, qryID, qID);
//      case WAIT_QUERY_FOR_QUEUE:
//        return new WaitRequest(context, sender, qryID, qID);
//      case SERVER_BUSY:
//        return new QueueBusyRequest(context, sender, qryID, qID);
      default:
        return null;
    }
  }
}
