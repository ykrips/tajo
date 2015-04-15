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

import org.apache.tajo.catalog.predicates.Predicate;
import org.apache.tajo.catalog.predicates.SubQuery;

public class ExistsPredicateImpl extends AbstractPredicateImpl implements Predicate {
  
  private final SubQuery subQuery;
  
  public ExistsPredicateImpl(SubQuery subQuery) {
    this.subQuery = subQuery;
  }

  @Override
  public String toSQLString() {
    StringBuilder existsBuilder = new StringBuilder();
    if (isNot()) {
      existsBuilder.append("NOT").append(" ");
    }
    existsBuilder.append("EXISTS").append(" ");
    existsBuilder.append("(").append(subQuery.toSQLString()).append(")");
    return existsBuilder.toString();
  }

}
