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

import org.apache.tajo.catalog.predicates.Expression;
import org.apache.tajo.catalog.predicates.Predicate;

public abstract class AbstractComparisonPredicateImpl extends AbstractPredicateImpl implements Predicate {

  private final Expression<?> leftSideExpression;
  
  private final Expression<?> rightSideExpression;
  
  private boolean equal;

  public AbstractComparisonPredicateImpl(Expression<?> leftSideExpression, Expression<?> rightSideExpression) {
    super();
    this.leftSideExpression = leftSideExpression;
    this.rightSideExpression = rightSideExpression;
    this.equal = false;
  }
  
  public Predicate equal() {
    this.equal = true;
    return this;
  }

  public Expression<?> getLeftSideExpression() {
    return leftSideExpression;
  }

  public Expression<?> getRightSideExpression() {
    return rightSideExpression;
  }

  public boolean isEqual() {
    return equal;
  }
  
}
