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
