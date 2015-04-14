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
import java.util.List;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.predicates.DBMSTable;
import org.apache.tajo.catalog.predicates.JoinTable;
import org.apache.tajo.catalog.predicates.Predicate;

public class JoinTableImpl extends DBMSTableImpl implements JoinTable {
  
  private JoinType joinType;
  
  private List<Predicate> predicates;
  
  private DBMSTable leftSide;
  
  private DBMSTable rightSide;

  public JoinTableImpl(DBMSTable leftSide, DBMSTable rightSide) {
    super(leftSide.getDatabaseName(), "join-"+System.currentTimeMillis());
    this.leftSide = leftSide;
    this.rightSide = rightSide;
    this.predicates = new ArrayList<Predicate>();
  }

  @Override
  public JoinType getJoinType() {
    return joinType;
  }

  @Override
  public List<Predicate> getPredicates() {
    return predicates;
  }

  @Override
  public DBMSTable getLeftSide() {
    return leftSide;
  }

  @Override
  public DBMSTable getRightSide() {
    return rightSide;
  }

  public JoinTable setJoinType(JoinType joinType) {
    this.joinType = joinType;
    return this;
  }

  public JoinTable setPredicates(List<Predicate> predicates) {
    this.predicates = predicates;
    return this;
  }

}
