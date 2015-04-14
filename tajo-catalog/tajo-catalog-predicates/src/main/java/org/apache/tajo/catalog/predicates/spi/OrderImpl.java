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
import java.util.List;

import org.apache.tajo.catalog.predicates.Expression;
import org.apache.tajo.catalog.predicates.Order;
import org.apache.tajo.util.TUtil;

public class OrderImpl implements Order {
  
  private boolean ascendingUsed;
  private final List<Expression<?>> expressionList = TUtil.newList();

  @Override
  public boolean isAscending() {
    return ascendingUsed;
  }
  
  public Order asc() {
    ascendingUsed = true;
    return this;
  }
  
  public Order desc() {
    ascendingUsed = false;
    return this;
  }

  @Override
  public List<Expression<?>> getExpression() {
    return expressionList;
  }
  
  public Order addExpression(Expression<?> expr) {
    expressionList.add(expr);
    return this;
  }
  
  public Order addExpressions(Collection<Expression<?>> exprs) {
    expressionList.addAll(exprs);
    return this;
  }
  
  @Override
  public String toSQLString() {
    StringBuilder queryBuilder = new StringBuilder();
    boolean first = true;
    for (Expression<?> expr: expressionList) {
      if (!(expr instanceof ColumnExpressionImpl)) {
        throw new IllegalArgumentException(expr.toSQLString() + " should be a column.");
      }
      
      if (!first) {
        queryBuilder.append(',');
      }
      queryBuilder.append(expr.toSQLString());
      first = false;
    }
    if (isAscending()) {
      queryBuilder.append(" ").append("ASC");
    } else {
      queryBuilder.append(" ").append("DESC");
    }
    return queryBuilder.toString();
  }

}
