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
