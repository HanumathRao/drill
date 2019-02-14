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

//import org.apache.drill.exec.proto.BitControl;
//import org.apache.drill.exec.proto.BitControl.SchedulingMessageType;
//import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
//import org.apache.drill.exec.server.DrillbitContext;
//
//import java.util.Map;
//import java.util.Queue;
//import org.apache.drill.exec.proto.UserBitShared.QueryId;
//
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;

public class QueueScheduler implements AutoCloseable {
//  private final RequestProcessor processor = null;
//  private final BlockingQueue<Request> responses;
//  private final DrillbitContext drillbitContext;
//
//  public static class RequestProcessor extends Thread {
//    private final Map<String, QueueContext> queueMap;
//    private final Queue<Request> requests;
//
//    public RequestProcessor(Map<String, QueueContext> queueMap) {
//      this.queueMap = queueMap;
//      this.requests = new LinkedBlockingQueue<>();
//    }
//
//    public boolean scheduleQuery(Request request) {
//      return requests.offer(request);
//    }
//
//    public final void run() {
//      while(true) {
//        Request request = requests.poll();
////        this.queueMap.get(request.queueId).process(request);
//      }
//    }
//  }
//
//  public static class ResponseProcessor extends Thread {
//
//  }
//
//  public QueueScheduler(DrillbitContext context) {
////    processor = new RequestProcessor(queueMap);
//    responses = new LinkedBlockingQueue<Request>();
//    drillbitContext = context;
//  }
//
//  public static Request makeRequest(QueueContext queueContext, DrillbitContext context, DrillbitEndpoint endpoint,
//                                    SchedulingMessageType type, QueryId qryID, Integer qID) {
//    return Request.RequestCreator.build(queueContext, context, type, endpoint, qryID, qID);
//  }
//
//  public boolean scheduleQuery(Request requestInfo) {
//    processor.scheduleQuery(requestInfo);
//    return true;
//  }
//
//  public void completed(Request requestInfo) {
//    Request nextRequest = null;
//    if (nextRequest.isValid()) {
//      responses.offer(nextRequest);
//    }
//  }
//
//  private void sendStatus(Request requestInfo) {
//    BitControl.QuerySchedulingMessage.Builder builder = BitControl.QuerySchedulingMessage.newBuilder();
////    builder.setQueueID(requestInfo.queueId);
////    builder.setQueryId(requestInfo.queryId);
////    drillbitContext.getController().getTunnel(requestInfo.sender).sendQueryAdmitMessage(null, builder.build());
////    System.out.println("sender: " + requestInfo.sender + "," + requestInfo.queueId + "," + requestInfo.queryId);
//  }
//
//  public final void run() {
//    try {
//      while (true) {
//        sendStatus(responses.take());
//      }
//    } catch (InterruptedException ex) {
//      System.out.println(ex);
//    }
//  }
//
  public void close() {
//    try {
//      processor.join(10);
//    } catch (InterruptedException ex) {
//
//    }
  }
}
