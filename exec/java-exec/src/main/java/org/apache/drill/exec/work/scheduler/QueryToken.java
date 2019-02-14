package org.apache.drill.exec.work.scheduler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to keep track of the validity of a Request.
 */
public class QueryToken implements Token<Request> {
  private final Request request;
  private final long startTime;
  private AtomicBoolean isValid;

  public QueryToken(Request req, long start) {
    this.request = req;
    this.startTime = start;
    this.isValid = new AtomicBoolean(true);
  }

  public long timeElapsed() {
    return System.currentTimeMillis() - this.startTime;
  }

  public boolean timeup(long timeOutValue) {
    return timeElapsed() > TimeUnit.SECONDS.toMillis(timeOutValue);
  }

  public void setValid(boolean validity) {
    isValid.set(validity);
  }

  public Request payload() {
    return request;
  }
}
