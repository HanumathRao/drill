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
   * checks if the validity for this token is expired.
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
