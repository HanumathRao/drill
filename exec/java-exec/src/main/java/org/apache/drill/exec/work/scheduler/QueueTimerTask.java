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

import java.util.Deque;
import java.util.Timer;
import java.util.TimerTask;

public abstract class QueueTimerTask extends TimerTask {
  private final long WAIT_TIME_BOUND;
  private final Deque<QueryToken> waitingQueries;
  private final Timer waitTimeTracker;

  public QueueTimerTask(Deque<QueryToken> tokenQueue, long waitTimeUpperBound) {
    WAIT_TIME_BOUND = waitTimeUpperBound;
    waitingQueries = tokenQueue;
    waitTimeTracker = new Timer(true);
  }

  public void offer(QueryToken token) {
    waitingQueries.offer(token);
  }

  public Token peek(){
    return waitingQueries.peek();
  }

  public void putFirst(QueryToken token) {
    waitingQueries.addFirst(token);
  }

  public abstract void doWork(QueryToken token);

  public abstract QueueTimerTask create(Deque<QueryToken> waitingQueue);

  public void run() {
    long nextWait;
    while (true) {
      QueryToken currentToken = waitingQueries.pop();
      if (!currentToken.timeup(WAIT_TIME_BOUND)) {
        nextWait = WAIT_TIME_BOUND - currentToken.timeElapsed();
        putFirst(currentToken);
        break;
      }
      doWork(currentToken);
    }
    waitTimeTracker.schedule(create(waitingQueries), nextWait);
  }
}
