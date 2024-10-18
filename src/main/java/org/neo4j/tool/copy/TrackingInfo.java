package org.neo4j.tool.copy;

public interface TrackingInfo {

  /**
   * Tracking the username for requests
   *
   * @return username of the request.
   */
  String getUsername();

  /**
   * Tracking the request through a particular transaction ID.
   *
   * @return ID for the transaction.
   */
  String getTransactionId();
}
