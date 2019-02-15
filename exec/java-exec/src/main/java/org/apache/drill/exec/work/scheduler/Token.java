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


/**
 * Token associates a validity period for a payload.
 * @param <T> represents the payload.
 */
public interface Token<T> {

  /**
   * elapsed time since the token is created.
   * @return elapsed time.
   */
  long timeElapsed();

  /**
   * checks the expiration of the token.
   * @param timeOutValue timeout value beyond which this
   *                     token is considered expired.
   * @return true/false based on the timeup of this token.
   */
  boolean timeup(long timeOutValue);

  /**
   * payload associated with the token.
   * @return payload for this token.
   */
  T payload();
}
