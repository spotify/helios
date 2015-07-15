/*
 * Copyright (c) 2015 Spotify AB.
 *
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

package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HostSelector extends Descriptor {

  // Use java.util.function.BiPredicate when available (java 8)
  private interface BiPredicate<T, U> {
    boolean test(T t, U u);
  }

  public enum Operator {
    EQUALS("=", new BiPredicate<String, Object>() {
      @Override
      public boolean test(final String a, final Object b) {
        return Objects.equals(a, b);
      }
    }),
    NOT_EQUALS("!=", new BiPredicate<String, Object>() {
      @Override
      public boolean test(final String a, final Object b) {
        return !Objects.equals(a, b);
      }
    });

    final String operatorName;
    final BiPredicate<String, Object> predicate;

    Operator(final String operatorName,
             final BiPredicate<String, Object> predicate) {
      this.operatorName = operatorName;
      this.predicate = predicate;
    }
  }

  private static final String LABEL_PATTERN = "[\\p{Alnum}\\._-]+";
  private static final String OPERAND_PATTERN = "[\\p{Alnum}\\._-]+";
  private static final Pattern PATTERN = Pattern.compile(
      format("^(%s)\\s*(!=|=)\\s*(%s)$", LABEL_PATTERN, OPERAND_PATTERN));

  private final String label;
  private final Operator operator;
  private final Object operand;

  public static HostSelector parse(final String str) {
    checkNotNull(str);

    final Matcher matcher = PATTERN.matcher(str);
    if (matcher.matches()) {
      final String label = matcher.group(1);
      final String opStr = matcher.group(2);
      final String value = matcher.group(3);

      Operator operator = null;
      for (final Operator op : Operator.values()) {
        if (op.operatorName.equals(opStr)) {
          operator = op;
        }
      }

      if (operator == null) {
        throw new IllegalArgumentException(format("Unknown operator '%s'", opStr));
      }

      return new HostSelector(label, operator, value);
    } else {
      return null;
    }
  }

  public HostSelector(
      @JsonProperty("label") final String label,
      @JsonProperty("operator") final Operator operator,
      @JsonProperty("operand") final Object operand) {
    this.label = label;
    this.operator = operator;
    this.operand = operand;
  }

  public String getLabel() {
    return label;
  }

  public Operator getOperator() {
    return operator;
  }

  public Object getOperand() {
    return operand;
  }

  /**
   * Check if the given value matches the host selectors.
   *
   * @param value Label value to test against.
   * @return True iff the value matches the host selector condition.
   */
  public boolean matches(@Nullable final String value) {
    return operator.predicate.test(value, operand);
  }

  /**
   * Return a human-readable string representation of the host selector. E.g. "A = B".
   *
   * @return A human-readable representation of the host selector.
   */
  public String toPrettyString() {
    return format("%s %s %s", label, operator.operatorName, operand.toString());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostSelector that = (HostSelector) o;

    if (label != null ? !label.equals(that.label) : that.label != null) {
      return false;
    }
    if (operand != null ? !operand.equals(that.operand) : that.operand != null) {
      return false;
    }
    if (operator != that.operator) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = label != null ? label.hashCode() : 0;
    result = 31 * result + (operator != null ? operator.hashCode() : 0);
    result = 31 * result + (operand != null ? operand.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "HostSelector{" +
           "label='" + label + '\'' +
           ", operator=" + operator +
           ", operand=" + operand +
           '}';
  }
}
