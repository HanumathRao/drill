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
