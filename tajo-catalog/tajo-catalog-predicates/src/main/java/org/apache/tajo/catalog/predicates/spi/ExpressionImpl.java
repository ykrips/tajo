/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog.predicates.spi;

import java.util.Collection;

import org.apache.tajo.catalog.predicates.Expression;
import org.apache.tajo.catalog.predicates.Predicate;

/**
 * 
 */
public abstract class ExpressionImpl<T> implements Expression<T> {
  
  private final Class<T> dataClass;
  
  public ExpressionImpl(Class<T> dataClass) {
    this.dataClass = dataClass;
  }
  
  protected Class<T> getDataClass() {
    return this.dataClass;
  }

  @Override
  public Predicate isNull() {
    return new IsNullPredicate(this);
  }

  @Override
  public Predicate isNotNull() {
    return new IsNullPredicate(this).not();
  }

  @Override
  public Predicate in(Object... values) {
    if (values == null || values.length == 0) {
      throw new IllegalArgumentException("values is null or empty.");
    }
    Class<?> inDataClass = values[0].getClass();
    return new InPredicate(inDataClass, this).addParameters(values);
  }

  @Override
  public Predicate in(Expression<?>... values) {
    if (values == null || values.length == 0) {
      throw new IllegalArgumentException("values is null or empty.");
    }
    Class<?> inDataClass = ((ExpressionImpl<?>)values[0]).getDataClass();
    return new InPredicate(inDataClass, this).addParameters(values);
  }

  @Override
  public Predicate in(Collection<?> values) {
    if (values == null || values.size() <= 0) {
      throw new IllegalArgumentException("values is null or empty.");
    }
    Class<?> inDataClass = values.iterator().next().getClass();
    return new InPredicate(inDataClass, this).addParameters(values);
  }

}
