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
