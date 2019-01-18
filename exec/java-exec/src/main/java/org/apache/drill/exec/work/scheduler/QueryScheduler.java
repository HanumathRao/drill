package org.apache.drill.exec.work.scheduler;

import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Queue;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryScheduler extends Thread {
  public enum QueryStatus {
    ADMITTED,
    WAITING,
    SERVER_BUSY
  }

  private final Map<Integer, WaitQueue> queueMap;
  private final BlockingQueue<Request> responses;
  private final DrillbitContext drillbitContext;
  private static Request NO_REQUEST = new Request(null, null, null);

  public QueryScheduler(DrillbitContext context) {
    setDaemon(true);
    queueMap = new HashMap<Integer, WaitQueue>();
    responses = new LinkedBlockingQueue<Request>();
    drillbitContext = context;
  }

  public static Request makeRequest(DrillbitEndpoint endpoint,
                                    QueryId qryID, Integer qID) {
    return new Request(endpoint, qryID, qID);
  }

  public boolean scheduleQuery(Request requestInfo) {
    queueMap.get(requestInfo.queueId).scheduleQuery(requestInfo);
    return true;
  }

  public void completed(Request requestInfo) {
    Request nextRequest = queueMap.get(requestInfo.queueId).finished(requestInfo);
    if (nextRequest.isValid()) {
      responses.offer(nextRequest);
    }
  }

  private void sendStatus(Request requestInfo) {
    BitControl.ScheduleQueryMessage.Builder builder = BitControl.ScheduleQueryMessage.newBuilder();
    builder.setQueueID(requestInfo.queueId);
    builder.setQueryId(requestInfo.queryId);
//    drillbitContext.getController().getTunnel(requestInfo.sender).sendQueryAdmitMessage(null, builder.build());
    System.out.println("sender: " + requestInfo.sender + "," +
      requestInfo.queueId + "," + requestInfo.queryId);
  }

  public final void run() {
    try {
      while (true) {
        sendStatus(responses.take());
      }
    } catch (InterruptedException ex) {
      System.out.println(ex);
    }
  }

  private static class WaitQueue {
    private final Set<QueryId> runningQueries;
    private final Queue<Request> waitingQueries;
    private final int runnableSize;
    private final int waitLimit;

    public WaitQueue(int size, int waitlimit) {
      runningQueries = new HashSet<QueryId>();
      waitingQueries = new LinkedList<Request>();
      runnableSize = size;
      waitLimit = waitlimit;
    }

    public synchronized QueryStatus scheduleQuery(Request request) {
      if (waitingQueries.size() == waitLimit) {
        return QueryStatus.SERVER_BUSY;
      }
      if (waitingQueries.size() > 0) {
        waitingQueries.offer(request);
        return QueryStatus.WAITING;
      } else {
        if (runningQueries.size() == runnableSize) {
          waitingQueries.offer(request);
          return QueryStatus.WAITING;
        } else {
          runningQueries.add(request.queryId);
          return QueryStatus.ADMITTED;
        }
      }
    }

    public synchronized Request finished(Request requestInfo) {
      runningQueries.remove(requestInfo.queryId);
      if (waitingQueries.size() > 0) {
        Request nextRequest = waitingQueries.poll();
        runningQueries.add(nextRequest.queryId);
        return nextRequest;
      }
      return NO_REQUEST;
    }
  }
}
