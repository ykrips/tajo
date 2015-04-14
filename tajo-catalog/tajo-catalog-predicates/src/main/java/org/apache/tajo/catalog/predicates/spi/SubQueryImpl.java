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
import org.apache.tajo.catalog.predicates.Predicate;
import org.apache.tajo.catalog.predicates.SubQuery;

public class SubQueryImpl implements SubQuery {
  
  private List<Expression<?>> selectExpr;
  
  private List<DBMSTable> fromTables;
  
  private List<Predicate> wherePredicates;
  
  private List<Expression<?>> groupByExpr;
  
  private List<Predicate> havingPredicates;
  
  public SubQueryImpl() {
    this.selectExpr = new ArrayList<Expression<?>>();
    this.fromTables = new ArrayList<DBMSTable>();
    this.wherePredicates = new ArrayList<Predicate>();
    this.groupByExpr = new ArrayList<Expression<?>>();
    this.havingPredicates = new ArrayList<Predicate>();
  }

  @Override
  public String toSQLString() {
    StringBuilder queryBuilder = new StringBuilder();
    if (selectExpr.size() > 0) {
      queryBuilder.append("SELECT").append(" ");
      queryBuilder.append(toStringBySelectExpr());
    }
    
    if (fromTables.size() > 0) {
      queryBuilder.append(" ").append("FROM").append(" ");
      queryBuilder.append(toStringByFromClause());
    }
    
    if (wherePredicates.size() > 0) {
      queryBuilder.append(" ").append("WHERE").append(" ");
      queryBuilder.append(toStringByWhereClause());
    }
    
    if (groupByExpr.size() > 0) {
      queryBuilder.append(" ").append("GROUP BY").append(" ");
      queryBuilder.append(toStringByGroupByClause());
    }
    
    if (havingPredicates.size() > 0) {
      queryBuilder.append(" ").append("HAVING").append(" ");
      queryBuilder.append(toStringByHavingClause());
    }
    return queryBuilder.toString();
  }
  
  private String toStringBySelectExpr() {
    StringBuilder selectBuilder = new StringBuilder();
    boolean first = true;
    
    for (Expression<?> expr: selectExpr) {
      if (!first) {
        selectBuilder.append(",");
      }
      selectBuilder.append(expr.toSQLString());
      first = false;
    }
    
    return selectBuilder.toString();
  }
  
  private String toStringByFromClause() {
    StringBuilder fromBuilder = new StringBuilder();
    boolean first = true;
    
    for (DBMSTable dbmsTable: fromTables) {
      if (!first) {
        fromBuilder.append(",");
      }
      fromBuilder.append(dbmsTable.toSQLString());
      first = false;
    }
    
    return fromBuilder.toString();
  }
  
  private String toStringByWhereClause() {
    StringBuilder whereBuilder = new StringBuilder();
    boolean first = true;
    
    for (Predicate predicate: wherePredicates) {
      if (!first) {
        whereBuilder.append(" ").append("AND").append(" ");
      }
      whereBuilder.append(predicate.toSQLString());
      first = false;
    }
    
    return whereBuilder.toString();
  }
  
  private String toStringByGroupByClause() {
    StringBuilder groupByBuilder = new StringBuilder();
    boolean first = true;
    
    for (Expression<?> expr: groupByExpr) {
      if (!first) {
        groupByBuilder.append(",");
      }
      groupByBuilder.append(expr.toSQLString());
      first = false;
    }
    
    return groupByBuilder.toString();
  }
  
  private String toStringByHavingClause() {
    StringBuilder havingBuilder = new StringBuilder();
    boolean first = true;
    
    for (Predicate predicate: havingPredicates) {
      if (!first) {
        havingBuilder.append(" ").append("AND").append(" ");
      }
      havingBuilder.append(predicate.toSQLString());
      first = false;
    }
    
    return havingBuilder.toString();
  }

  @Override
  public SubQuery select(Expression<?>... expr) {
    this.selectExpr.clear();
    this.selectExpr.addAll(Arrays.asList(expr));
    return this;
  }

  @Override
  public SubQuery select(List<Expression<?>> expr) {
    this.selectExpr.clear();
    this.selectExpr.addAll(expr);
    return this;
  }

  @Override
  public SubQuery from(DBMSTable... tables) {
    fromTables.addAll(Arrays.asList(tables));
    return this;
  }

  @Override
  public SubQuery join(DBMSTable left, DBMSTable right, JoinType joinType, Predicate... predicates) {
    fromTables.add(((JoinTableImpl) new JoinTableImpl(left, right).setJoinType(joinType))
        .setPredicates(Arrays.asList(predicates)));
    return this;
  }
  
  @Override
  public SubQuery where(Predicate... predicates) {
    wherePredicates.addAll(Arrays.asList(predicates));
    return this;
  }

  @Override
  public SubQuery groupBy(Expression<?>... expr) {
    groupByExpr.addAll(Arrays.asList(expr));
    return this;
  }

  @Override
  public SubQuery groupBy(List<Expression<?>> expr) {
    groupByExpr.addAll(expr);
    return this;
  }

  @Override
  public SubQuery having(Predicate... predicates) {
    havingPredicates.addAll(Arrays.asList(predicates));
    return this;
  }

}
