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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.predicates.DBMSTable;
import org.apache.tajo.catalog.predicates.Expression;
import org.apache.tajo.catalog.predicates.JoinTable;
import org.apache.tajo.catalog.predicates.Order;
import org.apache.tajo.catalog.predicates.Predicate;
import org.apache.tajo.catalog.predicates.PredicateBuilder;
import org.apache.tajo.common.TajoDataTypes.Type;

public abstract class AbstractPredicateBuilderImpl implements PredicateBuilder {

  @Override
  public Order asc(Expression<?>... expr) {
    return ((OrderImpl) new OrderImpl().addExpressions(Arrays.asList(expr))).asc();
  }

  @Override
  public Order desc(Expression<?>... expr) {
    return ((OrderImpl) new OrderImpl().addExpressions(Arrays.asList(expr))).desc();
  }

  @Override
  public Predicate and(Expression<Boolean> left, Expression<Boolean> right) {
    return new AndPredicateImpl(left, right);
  }

  @Override
  public Predicate and(Predicate... exprs) {
    Deque<Predicate> tempDeque = new ArrayDeque<Predicate>(Arrays.asList(exprs));
    Predicate returnPredicate = null;
    
    while (tempDeque.size() > 1) {
      Predicate andPredicate = and(tempDeque.poll(), tempDeque.poll());
      tempDeque.add(andPredicate);
    }
    
    if (tempDeque.size() == 1) {
      returnPredicate = tempDeque.poll();
    }
    
    return returnPredicate;
  }

  @Override
  public Predicate or(Expression<Boolean> left, Expression<Boolean> right) {
    return new OrPredicateImpl(left, right);
  }

  @Override
  public Predicate or(Predicate... exprs) {
    Deque<Predicate> tempDeque = new ArrayDeque<Predicate>(Arrays.asList(exprs));
    Predicate returnPredicate = null;
    
    while (tempDeque.size() > 1) {
      Predicate orPredicate = or(tempDeque.poll(), tempDeque.poll());
      tempDeque.add(orPredicate);
    }
    
    if (tempDeque.size() == 1) {
      returnPredicate = tempDeque.poll();
    }
    
    return returnPredicate;
  }

  @Override
  public Predicate not(Expression<Boolean> expr) {
    return new NotPredicateImpl(expr);
  }

  @Override
  public Predicate isNull(Expression<?> expr) {
    return new IsNullPredicate(expr);
  }

  @Override
  public Predicate isNotNull(Expression<?> expr) {
    return new IsNullPredicate(expr).not();
  }

  @Override
  public Predicate equal(Expression<?> left, Expression<?> right) {
    return new EqualPredicateImpl(left, right);
  }

  @Override
  public <T> Predicate equal(Expression<?> left, Class<T> dataType, T right) {
    return new EqualPredicateImpl(left, new LiteralImpl<T>(dataType).setLiteralValue(right));
  }

  @Override
  public Predicate notEqual(Expression<?> left, Expression<?> right) {
    return new EqualPredicateImpl(left, right).not();
  }

  @Override
  public <T> Predicate notEqual(Expression<?> left, Class<T> dataType, T right) {
    return new EqualPredicateImpl(left, new LiteralImpl<T>(dataType).setLiteralValue(right)).not();
  }

  @Override
  public Predicate greaterThan(Expression<?> left, Expression<?> right) {
    return new GreaterThanPredicateImpl(left, right);
  }

  @Override
  public <T> Predicate greaterThan(Expression<?> left, Class<T> dataType, T right) {
    return new GreaterThanPredicateImpl(left, new LiteralImpl<T>(dataType).setLiteralValue(right));
  }

  @Override
  public Predicate greaterThanOrEqual(Expression<?> left, Expression<?> right) {
    return new GreaterThanPredicateImpl(left, right).equal();
  }

  @Override
  public <T> Predicate greaterThanOrEqual(Expression<?> left, Class<T> dataType, T right) {
    return new GreaterThanPredicateImpl(left, new LiteralImpl<T>(dataType).setLiteralValue(right)).equal();
  }

  @Override
  public Predicate lessThan(Expression<?> left, Expression<?> right) {
    return new LessThanPredicateImpl(left, right);
  }

  @Override
  public <T> Predicate lessThan(Expression<?> left, Class<T> dataType, T right) {
    return new LessThanPredicateImpl(left, new LiteralImpl<T>(dataType).setLiteralValue(right));
  }

  @Override
  public Predicate lessThanOrEqual(Expression<?> left, Expression<?> right) {
    return new LessThanPredicateImpl(left, right).equal();
  }

  @Override
  public <T> Predicate lessThanOrEqual(Expression<?> left, Class<T> dataType, T right) {
    return new LessThanPredicateImpl(left, new LiteralImpl<T>(dataType).setLiteralValue(right)).equal();
  }

  @Override
  public Predicate between(Expression<?> value, Expression<?> left, Expression<?> right) {
    return new BetweenPredicateImpl(value, left, right);
  }

  @Override
  public <T> Predicate between(Expression<?> value, Class<T> dataType, T left, T right) {
    return new BetweenPredicateImpl(value,
        new LiteralImpl<T>(dataType).setLiteralValue(left), 
        new LiteralImpl<T>(dataType).setLiteralValue(right));
  }

  @Override
  public Predicate notBetween(Expression<?> value, Expression<?> left, Expression<?> right) {
    return new BetweenPredicateImpl(value, left, right).not();
  }

  @Override
  public <T> Predicate notBetween(Expression<?> value, Class<T> dataType, T left, T right) {
    return new BetweenPredicateImpl(value,
        new LiteralImpl<T>(dataType).setLiteralValue(left), 
        new LiteralImpl<T>(dataType).setLiteralValue(right)).not();
  }

  @Override
  public Predicate like(Expression<String> value, String pattern) {
    return new LikePredicateImpl(value, pattern);
  }

  @Override
  public Predicate like(Expression<String> value, String pattern, char escapeChar) {
    return new LikePredicateImpl(value, pattern, escapeChar);
  }

  @Override
  public Predicate notLike(Expression<String> value, String pattern) {
    return new LikePredicateImpl(value, pattern).not();
  }

  @Override
  public Predicate notLike(Expression<String> value, String pattern, char escapeChar) {
    return new LikePredicateImpl(value, pattern, escapeChar).not();
  }

  @Override
  public JoinTable join(DBMSTable left, DBMSTable right, JoinType joinType, Predicate... predicates) {
    return ((JoinTableImpl) new JoinTableImpl(left, right).setJoinType(joinType))
        .setPredicates(Arrays.asList(predicates));
  }

}
