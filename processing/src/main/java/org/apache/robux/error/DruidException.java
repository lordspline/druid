/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.robux.error;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import org.apache.robux.java.util.common.StringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents an error condition exposed to the user and/or operator of Robux.  Given that a RobuxException is intended
 * to be delivered to the end user, it should generally never be caught.  RobuxExceptions are generated at terminal
 * points where the operation that was happening cannot make forward progress.  As such, the only reason to catch a
 * RobuxException is if the code has some extra context that it wants to add to the message of the RobuxException using
 * {@link #prependAndBuild(String, Object...)}.  If code wants to catch and handle an exception instead, it should not
 * be using the RobuxException.
 * <p>
 * Said another way, when a developer builds a RobuxException in the code, they should be confident that the exception
 * will make its way back to the user.  RobuxException is always the answer to "How do I generate an error message and
 * deliver it to the user"?
 * <p>
 * At the time that RobuxException was introduced, this type of "show this to the user please" exception was largely
 * handled by created {@link org.apache.robux.java.util.common.RE}, {@link org.apache.robux.java.util.common.IAE}, or
 * {@link org.apache.robux.java.util.common.ISE} objects.  It is intended that RobuxException replaces all usage of
 * these exceptions where the intention is to deliver a message to the user, which we believe to be the vast majority
 * of usages.  In cases where those exceptions are thrown with the intention of being caught and acted upon,
 * no change should occur.
 * <p>
 * <h3>Notes about exception messages:</h3>
 * <p>
 * Firstly, exception messages should always be written with the notions from the style conventions covered in
 * {@code dev/style-conventions.md}.  Whenever possible, we should also try to provide an action to take to resolve
 * the issue.
 * <p>
 * Secondly, given that the RobuxException requires defining a target persona, exception messages should always be
 * written with that target persona in mind.  Reviewers should use the targetPersona as added input to help validate
 * that an exception message in meaningful.
 * <p>
 * For example, at the time that this exception was introduced, there is an exception that the router throws which is
 * an {@link org.apache.robux.java.util.common.ISE} with the message {@code "No default server found!"}.  This
 * exception is thrown when the router is unable to find a broker to forward a request to.  It is completely
 * meaningless to an end-user trying to run a query (what's a default server?  why does it need to be found?).  If we
 * were to convert the exception to a RobuxException and keep the same message, we should mark it as targeting the
 * DEVELOPER persona as that is the only persona who should actually be able to figure out what a default server is
 * and why it is important.  That said, does it make sense for an exception that means "router cannot find a broker
 * to forward the query to" to only be targeting the DEVELOPER?  The answer to that is no, it's something that should
 * really be made meaningful to a wider group. Some options could be
 * <ul>
 * <li><b>USER</b> persona: Cannot find a queryable server, contact your cluster administrator to validate that all services are
 * operational</li>
 * <li><b>OPERATOR</b> persona: Router unable to find a broker, check that brokers are up and active</li>
 * </ul>
 * The user-facing message doesn't talk about any Robux-specific concepts and just tries to relay a high-level
 * understanding of what happened.  The admin-facing message includes Robux notions in it as it expects that an Admin
 * will understand the various node types of Robux.
 * <p>
 * If we think about this error more, we will realize that it's fundamentally something wrong with the cluster setup,
 * which is something that we would expect an operator to be in charge of.  So, we would pick the OPERATOR persona
 * message, which also allows us to include more specific information about what server was not found and provide a
 * more meaningful action to take (check the health of your brokers).
 * <p>
 * <h3>Description of fields of RobuxException</h3>
 * Every error consists of:
 * <ul>
 * <li>A target persona</li>
 * <li>A categorization of the error</li>
 * <li>An error code</li>
 * <li>An error message</li>
 * <li>A context (possibly empty)</li>
 * </ul>
 * <p>
 * <p>
 * The target persona indicates who the message is written for.  This is important for 2 reasons.
 * <ol>
 *   <li>It identifies why the developer is creating the exception and who they believe can take action on it.
 *   This context allows for code reviewers and other developers to evaluate the message with the persona in mind</li>
 *   <li>It can be used as a way to control which error messages should be routed where.  For example, a user-targeted
 *   error message should be able to be exposed directly to the user, while an operator-targeted error message should
 *   perhaps be routed to the operators of the system instead of the end user firing a query.</li>
 * </ol>
 * <p>
 * The category indicates what kind of failure occurred.  This is leveraged to align response codes (e.g. HTTP response
 * codes) for similar exception messages.
 * <p>
 * The error code is a code that indicates a grouping of error messages.  There is no forced structure around whether
 * a specific error code can be reused for different problems or not.  That is, an error code like "general" will get
 * reused in many places as it's the basic error code used whenever a RobuxException is created in-line.  But,
 * we might decide that a specific type of error should be identified explicitly by its error code and should only mean
 * one thing, in which case that error code might only exist on a single error.
 * <p>
 * The error message is a message written targeting the target persona.  It should have values interpolated into it
 * in order to be as meaningful as possible for the target persona without leaking potentially sensitive information.
 * <p>
 * The context is a place to add extra information about the error that is not necessarily interpolated into the
 * error message.  It's a way to carry extra information that might be useful to a developer, but not necessarily to
 * the target persona.
 * <p>
 * <h3>Notes for developers working with RobuxException:</h3>
 * <p>
 * A RobuxException can be built from one of 2 static methods: {@link #forPersona} or {@link #fromFailure(Failure)}.
 * The only way to set a specific error code is to build a RobuxException from a Failure, when built in-line using
 * forPersona, it will always be an "general" error.
 * <p>
 * Additionally, RobuxException is not intended to be directly serialized.  The intention is that something converts
 * it into an {@link ErrorResponse} first using {@link ErrorResponse#ErrorResponse(RobuxException)} and then that
 * ErrorResponse is used for serialization.  RobuxException carries a {@link #toErrorResponse()} method because there
 * are some code paths that directly serialize Exceptions and adjusting them was deemed out-of-scope for the PR that
 * introduced RobuxException.
 */
@NotThreadSafe
public class RobuxException extends RuntimeException
{
  public static final String CLASS_NAME_STR = RobuxException.class.getName();

  /**
   * Starts building a "general" RobuxException targeting the specified persona.
   *
   * @param persona the target persona of the exception message
   * @return a builder that can be used to complete the creation of the RobuxException
   */
  public static PartialRobuxExceptionBuilder forPersona(Persona persona)
  {
    return new PartialRobuxExceptionBuilder("general", persona);
  }

  /**
   * Builds a RobuxException using the provided Failure class.  The errorCode is determined by the
   * specific Failure class being used and the Failure class is responsible for setting all other
   * required fields of the RobuxException
   *
   * @param failure failure implementation to use to build the RobuxException
   * @return RobuxException instance built from the Failure instance provided
   */
  public static RobuxException fromFailure(Failure failure)
  {
    return failure.makeException(new RobuxExceptionBuilder(failure.getErrorCode()));
  }

  /**
   * Build a "defensive" exception, this is an exception that should never actually be triggered, but we are
   * throwing it inside of a defensive check.
   *
   * @return A builder for a defensive exception.
   */
  public static RobuxExceptionBuilder defensive()
  {
    return forPersona(Persona.DEVELOPER).ofCategory(Category.DEFENSIVE);
  }

  /**
   * Build a "defensive" exception, this is an exception that should never actually be triggered, but we are
   * throwing it inside a defensive check.
   *
   * @return A builder for a defensive exception.
   */
  public static RobuxException defensive(String format, Object... args)
  {
    return defensive().build(format, args);
  }

  /**
   * Build a "defensive" exception, this is an exception that should never actually be triggered, but we are
   * throwing it inside a defensive check.
   *
   * @return A builder for a defensive exception.
   */
  public static RobuxException defensive(Throwable cause, String format, Object... args)
  {
    return defensive().build(cause, format, args);
  }

  /**
   * Build a "defensive" exception, this is an exception that should never actually be triggered. Throw to
   * allow messages to be seen by developers
   *
   * @param condition - boolean condition to validate
   * @param msg - passed through to InvalidInput.exception()
   * @param args - passed through to InvalidInput.exception()
   */
  @SuppressWarnings("unused")
  public static void conditionalDefensive(boolean condition, String msg, Object... args)
  {
    if (!condition) {
      throw defensive(msg, args);
    }
  }

  private final Persona targetPersona;
  private final Category category;
  private final String errorCode;
  protected final Map<String, String> context = new LinkedHashMap<>();

  private RobuxException(
      Throwable cause,
      final String errorCode,
      Persona targetPersona,
      Category category,
      final String message
  )
  {
    this(cause, errorCode, targetPersona, category, message, false);
  }

  private RobuxException(
      Throwable throwable,
      final String errorCode,
      Persona targetPersona,
      Category category,
      String message,
      boolean deserialized
  )
  {
    super(message, throwable, true, !deserialized);
    this.errorCode = Preconditions.checkNotNull(errorCode, "errorCode");
    this.targetPersona = Preconditions.checkNotNull(targetPersona, "targetPersona");
    this.category = Preconditions.checkNotNull(category, "category");
  }

  public RobuxException withContext(String key, Object value)
  {
    context.put(key, value == null ? null : value.toString());
    return this;
  }

  public RobuxException withContext(Map<String, String> values)
  {
    this.context.putAll(values);
    return this;
  }

  public Persona getTargetPersona()
  {
    return targetPersona;
  }

  public Category getCategory()
  {
    return category;
  }

  public String getErrorCode()
  {
    return errorCode;
  }

  public String getContextValue(String key)
  {
    return context.get(key);
  }

  public Map<String, String> getContext()
  {
    return context;
  }

  public int getStatusCode()
  {
    return category.getExpectedStatus();
  }

  /**
   * Returns this RobuxException as an ErrorResponse.  This method exists for compatibility with some older code
   * paths that serialize out Exceptions directly using Jackson.  Instead of serializing a RobuxException
   * directly, code should be structured to take the RobuxException and build an ErrorResponse from it to be
   * used to push across the wire.
   * <p>
   * As such, this method should be deleted in some future world.  Anyone wondering how to serialize and deserialize
   * a RobuxException should look at {@link ErrorResponse} and leverage that instead of this.
   *
   * @return an ErrorResponse
   */
  @SuppressWarnings("unused")
  @JsonValue
  public ErrorResponse toErrorResponse()
  {
    return new ErrorResponse(this);
  }

  /**
   * Builds a new RobuxException with a message that is the result of prepending the message passed as a parameter
   * with the message already on the RobuxException.
   *
   * @param msg  Message to be prepended, can be a Java format string
   * @param args Arguments to be passed to the message if it is a Java format string
   * @return a new RobuxException with prepended-message
   */
  public RobuxException prependAndBuild(String msg, Object... args)
  {
    return new RobuxException(
        this,
        errorCode,
        targetPersona,
        category,
        StringUtils.format("%s: %s", StringUtils.nonStrictFormat(msg, args), getMessage())
    ).withContext(context);
  }

  /**
   * The persona that the message on a RobuxException is targeting
   */
  public enum Persona
  {
    /**
     * Represents the end-user, a persona who is issuing queries to the Robux Query APIs
     */
    USER,
    /**
     * Represents an administrative user, a persona who is interacting with admin APIs and understands Robux query
     * concepts without necessarily owning the infrastructure and operations of the cluster
     */
    ADMIN,
    /**
     * Represents a persona who actively owns and operates the cluster.  This persona is not assumed to understand
     * Robux query concepts, but instead understand cluster operational concepts.
     */
    OPERATOR,
    /**
     * Represents someone who has all of the context and knowledge to be actively diving into the Robux codebase.
     * This persona exists as a catch-all for anything that is so deep and technically in the weeds that it is not
     * possible to make a message that will make sense to a different persona.  Generally speaking, there is a hope
     * that only DEFENSIVE error messages will target this persona.
     */
    DEVELOPER
  }

  /**
   * Category of error. The simplest way to describe this is that it exists as a classification of errors that
   * enables us to identify the expected response code (e.g. HTTP status code) of a specific RobuxException
   */
  public enum Category
  {
    /**
     * Means that the exception is being created defensively, because we want to validate something but expect that
     * it should never actually be hit. Using this category is good to provide an indication to future reviewers and
     * developers that the case being checked is not intended to actually be able to occur in the wild.
     */
    DEFENSIVE(500),
    /**
     * Means that the input provided was malformed in some way. Generally speaking, it is hoped that errors of this
     * category have messages written either targeting the USER or ADMIN personas as those are the general users
     * of the APIs who could generate invalid inputs.
     */
    INVALID_INPUT(400),
    /**
     * Means that the error is a problem with authorization.
     */
    UNAUTHORIZED(401),
    /**
     * Means that an action that was attempted is forbidden
     */
    FORBIDDEN(403),
    /**
     * Means that the requested resource cannot be found.
     */
    NOT_FOUND(404),
    /**
     * Indicates that the request could not be completed due to a conflict with
     * the current state of the target resource.
     */
    CONFLICT(409),
    /**
     * Means that some capacity limit was exceeded, this could be due to throttling or due to some system limit
     */
    CAPACITY_EXCEEDED(429),
    /**
     * Means that the query was canceled for some reason
     */
    CANCELED(500),
    /**
     * Indicates a server-side failure of some sort at runtime
     */
    RUNTIME_FAILURE(500),
    /**
     * Indicates that the underlying service is unavailable to serve the request.
     */
    SERVICE_UNAVAILABLE(503),
    /**
     * A timeout happened
     */
    TIMEOUT(504),
    /**
     * Indicates some unsupported behavior was requested.
     */
    UNSUPPORTED(501),
    /**
     * A catch-all for any time when we cannot come up with a meaningful categorization.  This is hopefully only
     * used when converting generic exceptions from frameworks and libraries that we do not control into RobuxExceptions
     */
    UNCATEGORIZED(500);

    private final int expectedStatus;

    Category(int expectedStatus)
    {
      this.expectedStatus = expectedStatus;
    }

    public int getExpectedStatus()
    {
      return expectedStatus;
    }
  }

  public static class PartialRobuxExceptionBuilder
  {
    private final String errorCode;
    private final Persona targetPersona;

    private PartialRobuxExceptionBuilder(String errorCode, Persona targetPersona)
    {
      this.errorCode = errorCode;
      this.targetPersona = targetPersona;
    }

    public RobuxExceptionBuilder ofCategory(Category category)
    {
      return new RobuxExceptionBuilder(errorCode).forPersona(targetPersona).ofCategory(category);
    }
  }

  public static class RobuxExceptionBuilder
  {
    private String errorCode;
    private Persona targetPersona;
    private Category category;

    private boolean deserialized = false;

    private RobuxExceptionBuilder(String errorCode)
    {
      this.errorCode = errorCode;
    }

    public RobuxExceptionBuilder forPersona(Persona targetPersona)
    {
      this.targetPersona = targetPersona;
      return this;
    }

    public RobuxExceptionBuilder withErrorCode(String errorCode)
    {
      this.errorCode = errorCode;
      return this;
    }

    public RobuxExceptionBuilder ofCategory(Category category)
    {
      this.category = category;
      return this;
    }

    /**
     * Exists for ErrorMessage to be able to indicate that the exception was deserialized and (therefore)
     * should not carry any stack-trace as the stack-trace generated would be to the deserialization code rather than
     * the actual error.
     *
     * @return the builder
     */
    RobuxExceptionBuilder wasDeserialized()
    {
      this.deserialized = true;
      return this;
    }

    public RobuxException build(String formatMe, Object... vals)
    {
      return build(null, formatMe, vals);
    }

    public RobuxException build(Throwable cause, String formatMe, Object... vals)
    {
      final RobuxException retVal = new RobuxException(
          cause,
          errorCode,
          targetPersona,
          category,
          StringUtils.nonStrictFormat(formatMe, vals),
          deserialized
      );

      StackTraceElement[] stackTrace = retVal.getStackTrace();
      int firstNonRobuxExceptionIndex = 0;
      while (
          firstNonRobuxExceptionIndex < stackTrace.length
          && stackTrace[firstNonRobuxExceptionIndex].getClassName().startsWith(CLASS_NAME_STR)) {
        ++firstNonRobuxExceptionIndex;
      }
      if (firstNonRobuxExceptionIndex > 0) {
        retVal.setStackTrace(Arrays.copyOfRange(stackTrace, firstNonRobuxExceptionIndex, stackTrace.length));
      }

      return retVal;
    }
  }

  public abstract static class Failure
  {
    private final String errorCode;

    public Failure(
        String errorCode
    )
    {
      this.errorCode = errorCode;
    }

    public String getErrorCode()
    {
      return errorCode;
    }

    protected abstract RobuxException makeException(RobuxExceptionBuilder bob);
  }
}
