package org.apache.drill.exec.work.scheduler;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.server.DrillbitContext;

public class QueryCancellationRequest extends Request {
  private final DrillbitContext drillbitContext;
  private final QueueContext queueContext;

  public QueryCancellationRequest(QueueContext queueContext,
                                  DrillbitContext context,
                                  DrillbitEndpoint sender,
                                  QueryId queryId, Integer queueId) {
    super(context, sender, queryId, queueId);
    drillbitContext = context;
    this.queueContext = queueContext;
  }

  @Override
  public void process() {
    synchronized (queueContext) {

    }
  }
}
