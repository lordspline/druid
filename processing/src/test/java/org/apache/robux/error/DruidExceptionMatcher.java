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

import org.apache.robux.matchers.RobuxMatchers;
import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.AllOf;

import java.util.ArrayList;

public class RobuxExceptionMatcher extends DiagnosingMatcher<Throwable>
{
  public static RobuxExceptionMatcher invalidInput()
  {
    return new RobuxExceptionMatcher(
        RobuxException.Persona.USER,
        RobuxException.Category.INVALID_INPUT,
        "invalidInput"
    );
  }

  public static RobuxExceptionMatcher unsupported()
  {
    return new RobuxExceptionMatcher(
        RobuxException.Persona.OPERATOR,
        RobuxException.Category.UNSUPPORTED,
        "general"
    );
  }

  public static RobuxExceptionMatcher invalidSqlInput()
  {
    return invalidInput().expectContext("sourceType", "sql");
  }

  public static RobuxExceptionMatcher internalServerError()
  {
    return new RobuxExceptionMatcher(
        RobuxException.Persona.OPERATOR,
        RobuxException.Category.RUNTIME_FAILURE,
        "internalServerError"
    );
  }

  public static RobuxExceptionMatcher forbidden()
  {
    return new RobuxExceptionMatcher(RobuxException.Persona.USER, RobuxException.Category.FORBIDDEN, "general");
  }

  public static RobuxExceptionMatcher conflict()
  {
    return new RobuxExceptionMatcher(
        RobuxException.Persona.OPERATOR,
        RobuxException.Category.CONFLICT,
        "general"
    );
  }

  public static RobuxExceptionMatcher defensive()
  {
    return new RobuxExceptionMatcher(
        RobuxException.Persona.DEVELOPER,
        RobuxException.Category.DEFENSIVE,
        "general"
    );
  }

  private final AllOf<RobuxException> delegate;
  private final ArrayList<Matcher<? super RobuxException>> matcherList;

  public RobuxExceptionMatcher(
      RobuxException.Persona targetPersona,
      RobuxException.Category category,
      String errorCode
  )
  {
    matcherList = new ArrayList<>();
    matcherList.add(RobuxMatchers.fn("targetPersona", RobuxException::getTargetPersona, Matchers.is(targetPersona)));
    matcherList.add(RobuxMatchers.fn("category", RobuxException::getCategory, Matchers.is(category)));
    matcherList.add(RobuxMatchers.fn("errorCode", RobuxException::getErrorCode, Matchers.is(errorCode)));

    delegate = new AllOf<>(matcherList);
  }

  public RobuxExceptionMatcher expectContext(String key, String value)
  {
    matcherList.add(0, RobuxMatchers.fn("context", RobuxException::getContext, Matchers.hasEntry(key, value)));
    return this;
  }

  public RobuxExceptionMatcher expectMessageIs(String s)
  {
    return expectMessage(Matchers.equalTo(s));
  }

  public RobuxExceptionMatcher expectMessageContains(String contains)
  {
    return expectMessage(Matchers.containsString(contains));
  }

  public RobuxExceptionMatcher expectMessage(Matcher<String> messageMatcher)
  {
    matcherList.add(0, RobuxMatchers.fn("message", RobuxException::getMessage, messageMatcher));
    return this;
  }

  public RobuxExceptionMatcher expectException(Matcher<Throwable> causeMatcher)
  {
    matcherList.add(0, RobuxMatchers.fn("cause", RobuxException::getCause, causeMatcher));
    return this;
  }

  @Override
  protected boolean matches(Object item, Description mismatchDescription)
  {
    return delegate.matches(item, mismatchDescription);
  }

  @Override
  public void describeTo(Description description)
  {
    delegate.describeTo(description);
  }

  public <T> void assertThrowsAndMatches(ThrowingSupplier fn)
  {
    boolean thrown = false;
    try {
      fn.get();
    }
    catch (Throwable e) {
      if (e instanceof RobuxException) {
        MatcherAssert.assertThat(e, this);
        thrown = true;
      } else {
        throw new RuntimeException(e);
      }
    }
    MatcherAssert.assertThat(thrown, Matchers.is(true));
  }

  public interface ThrowingSupplier
  {
    void get();
  }
}
