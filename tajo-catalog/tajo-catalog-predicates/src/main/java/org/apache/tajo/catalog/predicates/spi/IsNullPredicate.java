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

/**
 * This predicate implements the following case.
 * 
 * predicate ::= 'expression' IS [NOT] NULL
 */
public class IsNullPredicate extends AbstractPredicateImpl implements Predicate {
  
  private Expression<?> value;
  
  public IsNullPredicate(Expression<?> value) {
    super();
    this.value = value;
  }

  @Override
  public String toSQLString() {
    StringBuilder sqlBuiler = new StringBuilder();
    
    sqlBuiler.append(value.toSQLString()).append(" ")
      .append("IS");
    if (isNot()) {
      sqlBuiler.append(" ").append("NOT");
    }
    sqlBuiler.append(" ").append("NULL");
    return sqlBuiler.toString();
  }

}