package org.neo4j.tool.copy;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.sql.SQLTransientException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Retryables {

  // Neo4J Exceptions have special handling
  private static final List<Class<?>> RETRYABLE_CLASSES =
      ImmutableList.of(SQLTransientException.class, TimeoutException.class);

  // Connection error
  private static final List<Class<?>> CONNECTION_CLASSES = ImmutableList.of(ConnectException.class);

  private static final Logger log = LoggerFactory.getLogger(Retryables.class);

  /** Determine if this exception is retry-able. */
  public static boolean isRetryableError(Throwable error) {
    final Throwable cause = Throwables.getRootCause(error);
    log.trace("Checking if error {} can be retried", cause.getClass().getName());
    final boolean ret = isTransientException(cause);
    final String msg = ret ? "Error {} can be retried" : "Error {} can't be retried";
    log.trace(msg, cause.getClass().getName(), cause);
    return ret;
  }

  public static boolean isConnectionError(Throwable th) {
    log.trace("Checking if error {} is a connection error.", th.getClass().getName());
    final boolean ret =
        CONNECTION_CLASSES.stream().anyMatch(exceptionClass -> exceptionClass.isInstance(th));
    final String msg =
        ret ? "Error {} is a connection error." : "Error {} is not a connection error.";
    log.trace(msg, th.getClass().getName(), th);
    return ret;
  }

  private static boolean isTransientException(Throwable cause) {
    return (cause instanceof Neo4jException)
        ? isTransientNeo4jException((Neo4jException) cause)
        : RETRYABLE_CLASSES.stream().anyMatch(exceptionClass -> exceptionClass.isInstance(cause));
  }

  private static boolean isTransientNeo4jException(Neo4jException error) {
    // check for a specific type of exception
    if (error instanceof TransientException) {
      return true;
    }

    // retry neo4j is coming back or giant gc
    if (error instanceof ServiceUnavailableException) {
      return true;
    }

    final String message = error.getMessage();
    log.trace("Checking if neo4j error can be retried; message={}", message);
    if (message != null) {
      // Database constraints have changed (txId=%d) after this transaction (txId=%d) started, " +
      // which is not yet supported. Please retry your transaction to ensure all " + constraints are
      // executed.
      if (message.contains("Database constraints have changed")) {
        return true;
      }

      // The code for this error is 'Neo.DatabaseError.Statement.ExecutionFailed' which is too
      // generic. Checking the message only.
      if (message.contains("Could not compile query due to insanely frequent schema changes")) {
        return true;
      }
    }

    // Retries should not happen when transaction was explicitly terminated by the user. Termination
    // of transaction might result in two different error codes depending on where it was
    // terminated. These are really client errors but classification on the server is not entirely
    // correct, and they are classified as transient.
    final String code = error.code();
    log.trace("Checking if neo4j error can be retried; code={}", code);
    return code.startsWith("Neo.TransientError.")
        || code.equals("Neo.ClientError.Transaction.TransactionTimedOut")
        || code.equals("Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration")
        || code.equals("Neo.DatabaseError.Transaction.TransactionStartFailed")
        || code.equals("Neo.DatabaseError.Transaction.TransactionCommitFailed");
  }
}
