package org.apache.drill.exec.work.scheduler;

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.proto.BitControl.QuerySchedulingMessage;
import org.apache.drill.exec.proto.BitControl.SchedulingMessageType;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.server.DrillbitContext;

public abstract class Request implements Runnable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Request.class);

  public enum QueryStatus {
    ADMIT_QUERY,
    WAIT_QUERY,
    QUEUE_BUSY,
    NOT_A_LEADER
  }

  private final DrillbitContext drillbitContext;
  private final DrillbitEndpoint sender;
  private final QueryId queryId;
  private final Integer queueId;

  protected Request(DrillbitContext context,
    DrillbitEndpoint endpoint, QueryId qryID, Integer qID) {
    this.sender = endpoint;
    this.queryId = qryID;
    this.queueId = qID;
    this.drillbitContext = context;
  }

  public abstract void process();

  public void run() {
    process();
  }

  public boolean isValid() {
    return sender != null && queryId != null && queueId != null;
  }

  private SchedulingMessageType getMessageType(QueryStatus status) {
    switch(status) {
      case ADMIT_QUERY:
          return SchedulingMessageType.SCHEDULE_A_QUERY;
      case WAIT_QUERY:
          return SchedulingMessageType.WAIT_QUERY_FOR_QUEUE;
      case QUEUE_BUSY:
          return SchedulingMessageType.SERVER_BUSY;
      case NOT_A_LEADER:
          return SchedulingMessageType.NOT_A_LEADER_FOR_QUEUE;
      default:
          return null;
    }
  }

  protected void sendResponse(QueryStatus status, DrillbitEndpoint receiver) {
    QuerySchedulingMessage message = QuerySchedulingMessage.newBuilder()
                                                           .setStatus(getMessageType(status))
                                                           .setQueryId(queryId)
                                                           .setQueueID(queueId)
                                                           .setSender(receiver).build();

    drillbitContext.getController().getTunnel(sender).sendQueryAdmitMessage(new OutcomeListener(), message);
  }

  private class OutcomeListener implements RpcOutcomeListener<GeneralRPCProtos.Ack> {

    @Override
    public void failed(final RpcException ex) {
      logger.warn("Failed to inform upstream that receiver is finished");
    }

    @Override
    public void success(final GeneralRPCProtos.Ack value, final ByteBuf buffer) {
      // Do nothing
    }

    @Override
    public void interrupted(final InterruptedException e) {
      //Do nothing
    }
  }
}
