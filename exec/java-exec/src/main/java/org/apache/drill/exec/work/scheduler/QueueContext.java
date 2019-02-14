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

import org.apache.drill.exec.server.DrillbitContext;

import java.util.Deque;
import java.util.Queue;
import java.util.Timer;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class QueueContext {
  public static class ForemanInfo {
    private int runningCount;
    private int resourceUsage;

    public ForemanInfo(int running, int resource) {
      this.runningCount = running;
      this.resourceUsage = resource;
    }

    public int getResourceUsage() {
      return resourceUsage;
    }

    public int getRunningCount() {
      return runningCount;
    }
  }

  private static final int TIMEOUT_VALUE = 10;
  private static final int QUEUE_LIMIT = 10;
  private static final int MAX_RUN_LIMIT = 10;
  private final String queueName;
  private int totalRunningQueries;
  private final Map<String, ForemanInfo> foremanInfoMap;
  private final BlockingDeque<QueryToken> waitingQueries;
  private final DrillbitContext drillbitContext;
  private final Timer waitTimeTracker;
  private final Queue<Request> timedOutRequests;

  public QueueContext(String name,
                      DrillbitContext context,
                      Queue<Request> timeoutrequests) {
    queueName = name;
    drillbitContext = context;
    waitingQueries = new LinkedBlockingDeque<>();
    timedOutRequests = timeoutrequests;
    waitTimeTracker = new Timer(true);
    waitTimeTracker.schedule(new TimeoutWatcher(waitingQueries, timedOutRequests), 0);
    this.foremanInfoMap = new HashMap<>();
  }

  class TimeoutWatcher extends QueueTimerTask {
    private final Queue<Request> timedOutRequests;

    public TimeoutWatcher(Deque<QueryToken> waitingQueue, Queue<Request> timedOutQueue) {
      super(waitingQueue, TIMEOUT_VALUE);
      timedOutRequests = timedOutQueue;
    }

    public void doWork(QueryToken removed) {
      timedOutRequests.offer(removed.payload());
    }

    @Override
    public QueueTimerTask create(Deque<QueryToken> waitingQueue) {
      return new TimeoutWatcher(waitingQueue, timedOutRequests);
    }
  }

  public int getNumOfWaitingQueries() {
    return waitingQueries.size();
  }

  public void offerInWaitingQueue(Request request) {
    waitingQueries.offer(new QueryToken(request, System.currentTimeMillis()));
  }

  public Request getWaitingRequest() {
    QueryToken token = waitingQueries.poll();
    return token != null ? token.payload() : null;
  }

  public int getNumOfRunningQueries() {
    return totalRunningQueries;
  }

  public void finishRunningQuery() {
    totalRunningQueries--;
  }

  public void offerInRunningQueue() {
    totalRunningQueries += 1;
  }
}
