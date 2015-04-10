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
import org.apache.tajo.common.TajoDataTypes;

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
  
  // check null
  Predicate isNull(Expression<?> expr);
  
  Predicate isNotNull(Expression<?> expr);
  
  // comparing operators
  Predicate equal(Expression<?> left, Expression<?> right);
  
  Predicate equal(Expression<?> left, TajoDataTypes.Type dataType, Object right);
  
  Predicate notEqual(Expression<?> left, Expression<?> right);
  
  Predicate notEqual(Expression<?> left, TajoDataTypes.Type dataType, Object right);
  
  Predicate greaterThan(Expression<?> left, Expression<?> right);
  
  Predicate greaterThan(Expression<?> left, TajoDataTypes.Type dataType, Object right);
  
  Predicate greaterThanOrEqual(Expression<?> left, Expression<?> right);
  
  Predicate greaterThanOrEqual(Expression<?> left, TajoDataTypes.Type dataType, Object right);
  
  Predicate lessThan(Expression<?> left, Expression<?> right);
  
  Predicate lessThan(Expression<?> left, TajoDataTypes.Type dataType, Object right);
  
  Predicate lessThanOrEqual(Expression<?> left, Expression<?> right);
  
  Predicate lessThanOrEqual(Expression<?> left, TajoDataTypes.Type dataType, Object right);
  
  Predicate between(Expression<?> value, Expression<?> left, Expression<?> right);
  
  Predicate between(Expression<?> value, TajoDataTypes.Type dataType, Object left, Object right);
  
  Predicate notBetween(Expression<?> value, Expression<?> left, Expression<?> right);
  
  Predicate notBetween(Expression<?> value, TajoDataTypes.Type dataType, Object left, Object right);
  
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
  
}
