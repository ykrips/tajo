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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.tajo.catalog.predicates.Expression;
import org.apache.tajo.catalog.predicates.Predicate;

/**
 * Predicate ::= 'expression' in ('expression', ..., 'expression')
 */
public class InPredicate extends AbstractPredicateImpl implements Predicate {
  
  private final Class<?> inDataClass;
  private Expression<?> valueExpression;
  private final List<Expression<?>> parameterList;

  public InPredicate(Class<?> dataClass, Expression<?> value) {
    super();
    this.inDataClass = dataClass;
    this.valueExpression = value;
    parameterList = new ArrayList<Expression<?>>();
  }
  
  public Predicate addParameters(Object... values) {
    appendParameterList(Arrays.asList(values));
    return this;
  }
  
  public Predicate addParameters(Expression<?>... values) {
    for (Expression<?> value: values) {
      parameterList.add(value);
    }
    return this;
  }
  
  public Predicate addParameters(Collection<?> values) {
    appendParameterList(values);
    return this;
  }
  
  private void appendParameterList(Collection<?> values) {
    for (Object value: values) {
      if (inDataClass.isAssignableFrom(value.getClass())) {
        parameterList.add(new LiteralImpl(inDataClass).setLiteralValue(value));
      } else {
        throw new IllegalArgumentException("This value (" + value + ") is not assignable to " + 
            inDataClass.getName());
      }
    }
  }

  @Override
  public String toSQLString() {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(valueExpression.toSQLString())
      .append(" ").append("IN").append(" ").append("(");
    boolean first = true;
    for (Expression<?> param: parameterList) {
      if (!first) {
        sqlBuilder.append(",");
      }
      sqlBuilder.append(param.toSQLString());
      first = false;
    }
    sqlBuilder.append(")");
    return sqlBuilder.toString();
  }
}
