package org.apache.drill.exec.work.scheduler;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;

public class Request {
  public final DrillbitEndpoint sender;
  public final QueryId queryId;
  public final Integer queueId;

  public Request(DrillbitEndpoint endpoint, QueryId qryID, Integer qID) {
    this.sender = endpoint;
    this.queryId = qryID;
    this.queueId = qID;
  }

  public boolean isValid() {
    return sender != null && queryId != null && queueId != null;
  }
}
