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
