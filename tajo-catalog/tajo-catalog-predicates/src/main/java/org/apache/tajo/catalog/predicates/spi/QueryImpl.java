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
import java.util.List;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.predicates.DBMSTable;
import org.apache.tajo.catalog.predicates.Expression;
import org.apache.tajo.catalog.predicates.Order;
import org.apache.tajo.catalog.predicates.Predicate;
import org.apache.tajo.catalog.predicates.Query;
import org.apache.tajo.catalog.predicates.SubQuery;

public class QueryImpl implements Query {
  
  private final SubQuery subQuery;
  
  private final List<Order> orderClause;
  
  public QueryImpl() {
    this.subQuery = new SubQueryImpl();
    this.orderClause = new ArrayList<Order>();
  }

  @Override
  public String toSQLString() {
    StringBuilder queryBuilder = new StringBuilder(subQuery.toSQLString());
    
    if (this.orderClause.size() > 0) {
      queryBuilder.append(" ").append("ORDER BY").append(" ");
      queryBuilder.append(toStringByOrderByClause());
    }
    
    return queryBuilder.toString();
  }
  
  private String toStringByOrderByClause() {
    StringBuilder orderBuilder = new StringBuilder();
    boolean first = true;
    
    for (Order order: orderClause) {
      if (!first) {
        orderBuilder.append(",");
      }
      orderBuilder.append(order.toSQLString());
      first = false;
    }
    
    return orderBuilder.toString();
  }

  @Override
  public Query select(Expression<?>... expr) {
    subQuery.select(expr);
    return this;
  }

  @Override
  public Query select(List<Expression<?>> expr) {
    subQuery.select(expr);
    return this;
  }

  @Override
  public Query from(DBMSTable... tables) {
    subQuery.from(tables);
    return this;
  }

  @Override
  public Query join(DBMSTable left, DBMSTable right, JoinType joinType, Predicate... predicates) {
    subQuery.join(left, right, joinType, predicates);
    return this;
  }

  @Override
  public Query where(Predicate... predicates) {
    subQuery.where(predicates);
    return this;
  }

  @Override
  public Query groupBy(Expression<?>... expr) {
    subQuery.groupBy(expr);
    return this;
  }

  @Override
  public Query groupBy(List<Expression<?>> expr) {
    subQuery.groupBy(expr);
    return this;
  }

  @Override
  public Query having(Predicate... predicates) {
    subQuery.having(predicates);
    return this;
  }
  
  @Override
  public Query orderBy(Order... orders) {
    orderClause.addAll(Arrays.asList(orders));
    return this;
  }

}
