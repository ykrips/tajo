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

package org.apache.tajo.catalog.predicates;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.tajo.algebra.JoinType;

/**
 * Create a simple query builder using some predicate clauses.
 */
public interface PredicateBuilder {

  // ordering
  Order asc(Expression<?>... expr);
  
  Order desc(Expression<?>... expr);
  
  // boolean operators
  Predicate and(Expression<Boolean> left, Expression<Boolean> right);
  
  Predicate and(Predicate... exprs);
  
  Predicate or(Expression<Boolean> left, Expression<Boolean> right);
  
  Predicate or(Predicate... exprs);
  
  Predicate not(Expression<Boolean> expr);
  
  Predicate parentheses(Expression<Boolean> expr);
  
  // check null
  Predicate isNull(Expression<?> expr);
  
  Predicate isNotNull(Expression<?> expr);
  
  // comparing operators
  Predicate equal(Expression<?> left, Expression<?> right);
  
  <T> Predicate equal(Expression<?> left, Class<T> dataType, T right);
  
  Predicate notEqual(Expression<?> left, Expression<?> right);
  
  <T> Predicate notEqual(Expression<?> left, Class<T> dataType, T right);
  
  Predicate greaterThan(Expression<?> left, Expression<?> right);
  
  <T> Predicate greaterThan(Expression<?> left, Class<T> dataType, T right);
  
  Predicate greaterThanOrEqual(Expression<?> left, Expression<?> right);
  
  <T> Predicate greaterThanOrEqual(Expression<?> left, Class<T> dataType, T right);
  
  Predicate lessThan(Expression<?> left, Expression<?> right);
  
  <T> Predicate lessThan(Expression<?> left, Class<T> dataType, T right);
  
  Predicate lessThanOrEqual(Expression<?> left, Expression<?> right);
  
  <T> Predicate lessThanOrEqual(Expression<?> left, Class<T> dataType, T right);
  
  Predicate between(Expression<?> value, Expression<?> left, Expression<?> right);
  
  <T> Predicate between(Expression<?> value, Class<T> dataType, T left, T right);
  
  Predicate notBetween(Expression<?> value, Expression<?> left, Expression<?> right);
  
  <T> Predicate notBetween(Expression<?> value, Class<T> dataType, T left, T right);
  
  Predicate like(Expression<String> value, String pattern);
  
  Predicate like(Expression<String> value, String pattern, char escapeChar);
  
  Predicate notLike(Expression<String> value, String pattern);
  
  Predicate notLike(Expression<String> value, String pattern, char escapeChar);
  
  // datetime
  Expression<Date> currentDate();
  
  Expression<Timestamp> currentTimestamp();
  
  Expression<Time> currentTime();
  
  // join
  JoinTable join(DBMSTable left, DBMSTable right, JoinType joinType, Predicate... predicates);
  
  // exists
  Predicate exists(SubQuery subQuery);
  
  Predicate notExists(SubQuery subQuery);
  
  // column expression
  <T> Expression<T> column(String canonicalName, Class<T> dataClass);
  
  <T> Expression<T> column(String tableName, String columnName, Class<T> dataClass);
  
  // literal
  <T> Expression<T> literal(Class<T> dataClass, T data);
  
}
