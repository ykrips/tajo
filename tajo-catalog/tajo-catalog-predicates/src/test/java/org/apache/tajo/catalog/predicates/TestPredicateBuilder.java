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

import static org.junit.Assert.*;

import org.apache.tajo.catalog.predicates.QueryBuilderFactory.DBMSType;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPredicateBuilder {
  
  private static PredicateBuilder predicateBuilder;
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    predicateBuilder = QueryBuilderFactory.getInstance().getPredicateBuilder(DBMSType.Derby);
  }
  
  @Test
  public void testAscendingOrder() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    
    Order ascOrder = predicateBuilder.asc(intColumnA, varcharColumnB);
    
    assertNotNull(ascOrder);
    assertTrue(ascOrder.isAscending());
    assertEquals(2, ascOrder.getExpression().size());
    assertEquals(intColumnA, ascOrder.getExpression().get(0));
    assertEquals(varcharColumnB, ascOrder.getExpression().get(1));
    assertEquals("test1.A,test2.B ASC", ascOrder.toSQLString());
  }
  
  @Test
  public void testDescendingOrder() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    
    Order descOrder = predicateBuilder.desc(intColumnA, varcharColumnB);
    
    assertNotNull(descOrder);
    assertFalse(descOrder.isAscending());
    assertEquals(2, descOrder.getExpression().size());
    assertEquals(intColumnA, descOrder.getExpression().get(0));
    assertEquals(varcharColumnB, descOrder.getExpression().get(1));
    assertEquals("test1.A,test2.B DESC", descOrder.toSQLString());
  }
  
  @Test
  public void testAndPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate isNotNullPredicate = predicateBuilder.isNotNull(varcharColumnB);
    assertNotNull(isNotNullPredicate);
    Predicate equalPredicate = predicateBuilder.equal(intColumnA, literalA);
    assertNotNull(equalPredicate);
    Predicate andPredicate = predicateBuilder.and(isNotNullPredicate, equalPredicate);
    assertNotNull(andPredicate);
    
    assertEquals("test2.B IS NOT NULL AND test1.A = 25", andPredicate.toSQLString());
  }
  
  @Test
  public void testAndPredicatesWithMultipleExpressions() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    Expression<String> varcharColumnC = predicateBuilder.column("test3", "C", String.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    Expression<String> literalB = predicateBuilder.literal(String.class, "TestB");
    
    Predicate equalIntegerPredicate = predicateBuilder.equal(intColumnA, literalA);
    assertNotNull(equalIntegerPredicate);
    Predicate equalStringPredicate = predicateBuilder.equal(varcharColumnB, literalB);
    assertNotNull(equalStringPredicate);
    Predicate isNullPredicate = predicateBuilder.isNull(varcharColumnC);
    assertNotNull(isNullPredicate);
    Predicate andPredicate = predicateBuilder.and(equalIntegerPredicate, equalStringPredicate, isNullPredicate);
    assertNotNull(andPredicate);
    
    assertEquals("test1.A = 25 AND test2.B = 'TestB' AND test3.C IS NULL", andPredicate.toSQLString());
  }
  
  @Test
  public void testOrPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate isNotNullPredicate = predicateBuilder.isNotNull(varcharColumnB);
    assertNotNull(isNotNullPredicate);
    Predicate equalPredicate = predicateBuilder.equal(intColumnA, literalA);
    assertNotNull(equalPredicate);
    Predicate orPredicate = predicateBuilder.or(isNotNullPredicate, equalPredicate);
    assertNotNull(orPredicate);
    
    assertEquals("test2.B IS NOT NULL OR test1.A = 25", orPredicate.toSQLString());
  }
  
  @Test
  public void testOrPredicatesWithMultipleExpressions() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    Expression<String> varcharColumnC = predicateBuilder.column("test3", "C", String.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    Expression<String> literalB = predicateBuilder.literal(String.class, "TestB");
    
    Predicate equalIntegerPredicate = predicateBuilder.equal(intColumnA, literalA);
    assertNotNull(equalIntegerPredicate);
    Predicate equalStringPredicate = predicateBuilder.equal(varcharColumnB, literalB);
    assertNotNull(equalStringPredicate);
    Predicate isNullPredicate = predicateBuilder.isNull(varcharColumnC);
    assertNotNull(isNullPredicate);
    Predicate orPredicate = predicateBuilder.or(equalIntegerPredicate, equalStringPredicate, isNullPredicate);
    assertNotNull(orPredicate);
    
    assertEquals("test1.A = 25 OR test2.B = 'TestB' OR test3.C IS NULL", orPredicate.toSQLString());
  }

  @Test
  public void testNotPredicate() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate greaterThanPredicate = predicateBuilder.greaterThan(intColumnA, literalA);
    assertNotNull(greaterThanPredicate);
    Predicate notPredicate = predicateBuilder.not(greaterThanPredicate);
    assertNotNull(notPredicate);
    
    assertEquals("NOT test1.A > 25", notPredicate.toSQLString());
  }
  
  @Test
  public void testMixedBooleanOperatorPredicate() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    Expression<String> varcharColumnC = predicateBuilder.column("test3", "C", String.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    Expression<String> literalB = predicateBuilder.literal(String.class, "TestB");
    
    Predicate equalIntegerPredicate = predicateBuilder.equal(intColumnA, literalA);
    assertNotNull(equalIntegerPredicate);
    Predicate equalStringPredicate = predicateBuilder.equal(varcharColumnB, literalB);
    assertNotNull(equalStringPredicate);
    Predicate isNullPredicate = predicateBuilder.isNull(varcharColumnC);
    assertNotNull(isNullPredicate);
    Predicate andPredicate = predicateBuilder.and(equalIntegerPredicate, equalStringPredicate);
    assertNotNull(andPredicate);
    andPredicate = predicateBuilder.parentheses(andPredicate);
    assertNotNull(andPredicate);
    Predicate orPredicate = predicateBuilder.or(andPredicate, isNullPredicate);
    assertNotNull(orPredicate);
    
    assertEquals("(test1.A = 25 AND test2.B = 'TestB') OR test3.C IS NULL", orPredicate.toSQLString());
  }
  
  @Test
  public void testEqualPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    Expression<String> varcharColumnC = predicateBuilder.column("test3", "C", String.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate equalPredicate = null;
    equalPredicate = predicateBuilder.equal(intColumnA, literalA);
    assertNotNull(equalPredicate);
    assertEquals("test1.A = 25", equalPredicate.toSQLString());
    
    equalPredicate = predicateBuilder.equal(varcharColumnB, String.class, "TestB");
    assertNotNull(equalPredicate);
    assertEquals("test2.B = 'TestB'", equalPredicate.toSQLString());
    
    equalPredicate = predicateBuilder.equal(varcharColumnC, String.class, "TestC");
    assertNotNull(equalPredicate);
    assertEquals("test3.C = 'TestC'", equalPredicate.toSQLString());
  }
  
  @Test
  public void testNotEqualPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<String> varcharColumnB = predicateBuilder.column("test2", "B", String.class);
    Expression<String> varcharColumnC = predicateBuilder.column("test3", "C", String.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate notEqualPredicate = null;
    notEqualPredicate = predicateBuilder.notEqual(intColumnA, literalA);
    assertNotNull(notEqualPredicate);
    assertEquals("test1.A <> 25", notEqualPredicate.toSQLString());
    
    notEqualPredicate = predicateBuilder.notEqual(varcharColumnB, String.class, "TestB");
    assertNotNull(notEqualPredicate);
    assertEquals("test2.B <> 'TestB'", notEqualPredicate.toSQLString());
    
    notEqualPredicate = predicateBuilder.notEqual(varcharColumnC, String.class, "TestC");
    assertNotNull(notEqualPredicate);
    assertEquals("test3.C <> 'TestC'", notEqualPredicate.toSQLString());
  }
  
  @Test
  public void testGreaterThanPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<Float> floatColumnB = predicateBuilder.column("test2", "B", Float.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate greaterThanPredicate = null;
    greaterThanPredicate = predicateBuilder.greaterThan(intColumnA, literalA);
    assertNotNull(greaterThanPredicate);
    assertEquals("test1.A > 25", greaterThanPredicate.toSQLString());
    
    greaterThanPredicate = predicateBuilder.greaterThan(floatColumnB, Float.class, 4.9f);
    assertNotNull(greaterThanPredicate);
    assertEquals("test2.B > 4.9", greaterThanPredicate.toSQLString());
  }
  
  @Test
  public void testGreaterThanOrEqualPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<Float> floatColumnB = predicateBuilder.column("test2", "B", Float.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate greaterThanOrEqualPredicate = null;
    greaterThanOrEqualPredicate = predicateBuilder.greaterThanOrEqual(intColumnA, literalA);
    assertNotNull(greaterThanOrEqualPredicate);
    assertEquals("test1.A >= 25", greaterThanOrEqualPredicate.toSQLString());
    
    greaterThanOrEqualPredicate = predicateBuilder.greaterThanOrEqual(floatColumnB, Float.class, 4.9f);
    assertNotNull(greaterThanOrEqualPredicate);
    assertEquals("test2.B >= 4.9", greaterThanOrEqualPredicate.toSQLString());
  }
  
  @Test
  public void testLessThanPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<Float> floatColumnB = predicateBuilder.column("test2", "B", Float.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate lessThanPredicate = null;
    lessThanPredicate = predicateBuilder.lessThan(intColumnA, literalA);
    assertNotNull(lessThanPredicate);
    assertEquals("test1.A < 25", lessThanPredicate.toSQLString());
    
    lessThanPredicate = predicateBuilder.lessThan(floatColumnB, Float.class, 4.9f);
    assertNotNull(lessThanPredicate);
    assertEquals("test2.B < 4.9", lessThanPredicate.toSQLString());
  }
  
  @Test
  public void testLessThanOrEqualPredicates() throws Exception {
    Expression<Integer> intColumnA = predicateBuilder.column("test1", "A", Integer.class);
    Expression<Float> floatColumnB = predicateBuilder.column("test2", "B", Float.class);
    Expression<Integer> literalA = predicateBuilder.literal(Integer.class, 25);
    
    Predicate lessThanOrEqualPredicate = null;
    lessThanOrEqualPredicate = predicateBuilder.lessThanOrEqual(intColumnA, literalA);
    assertNotNull(lessThanOrEqualPredicate);
    assertEquals("test1.A <= 25", lessThanOrEqualPredicate.toSQLString());
    
    lessThanOrEqualPredicate = predicateBuilder.lessThanOrEqual(floatColumnB, Float.class, 4.9f);
    assertNotNull(lessThanOrEqualPredicate);
    assertEquals("test2.B <= 4.9", lessThanOrEqualPredicate.toSQLString());
  }
  
  @Test
  public void testBetweenPredicates() throws Exception {
    Expression<String> varcharColumnA = predicateBuilder.column("test1", "A", String.class);
    Expression<String> literalA1 = predicateBuilder.literal(String.class, "A1");
    Expression<String> literalA2 = predicateBuilder.literal(String.class, "A2");
    
    Predicate betweenPredicate = null;
    betweenPredicate = predicateBuilder.between(varcharColumnA, literalA1, literalA2);
    assertNotNull(betweenPredicate);
    assertEquals("test1.A BETWEEN 'A1' AND 'A2'", betweenPredicate.toSQLString());
    
    betweenPredicate = predicateBuilder.between(varcharColumnA, String.class, "TestA", "TestB");
    assertNotNull(betweenPredicate);
    assertEquals("test1.A BETWEEN 'TestA' AND 'TestB'", betweenPredicate.toSQLString());
  }
  
  @Test
  public void testNotBetweenPredicates() throws Exception {
    Expression<String> varcharColumnA = predicateBuilder.column("test1", "A", String.class);
    Expression<String> literalA1 = predicateBuilder.literal(String.class, "A1");
    Expression<String> literalA2 = predicateBuilder.literal(String.class, "A2");
    
    Predicate notBetweenPredicate = null;
    notBetweenPredicate = predicateBuilder.notBetween(varcharColumnA, literalA1, literalA2);
    assertNotNull(notBetweenPredicate);
    assertEquals("test1.A NOT BETWEEN 'A1' AND 'A2'", notBetweenPredicate.toSQLString());
    
    notBetweenPredicate = predicateBuilder.notBetween(varcharColumnA, String.class, "TestA", "TestB");
    assertNotNull(notBetweenPredicate);
    assertEquals("test1.A NOT BETWEEN 'TestA' AND 'TestB'", notBetweenPredicate.toSQLString());
  }
  
  @Test
  public void testLikePredicates() throws Exception {
    Expression<String> varcharColumnA = predicateBuilder.column("test1", "A", String.class);
    
    Predicate likePredicate = null;
    likePredicate = predicateBuilder.like(varcharColumnA, "Sant%");
    assertNotNull(likePredicate);
    assertEquals("test1.A LIKE 'Sant%'", likePredicate.toSQLString());
    
    likePredicate = predicateBuilder.like(varcharColumnA, "Sant!%%", '!');
    assertNotNull(likePredicate);
    assertEquals("test1.A LIKE 'Sant!%%' ESCAPE '!'", likePredicate.toSQLString());
  }
  
  @Test
  public void testNotLikePredicates() throws Exception {
    Expression<String> varcharColumnA = predicateBuilder.column("test1", "A", String.class);
    
    Predicate notLikePredicate = null;
    notLikePredicate = predicateBuilder.notLike(varcharColumnA, "Sant%");
    assertNotNull(notLikePredicate);
    assertEquals("test1.A NOT LIKE 'Sant%'", notLikePredicate.toSQLString());
    
    notLikePredicate = predicateBuilder.notLike(varcharColumnA, "Sant!%%", '!');
    assertNotNull(notLikePredicate);
    assertEquals("test1.A NOT LIKE 'Sant!%%' ESCAPE '!'", notLikePredicate.toSQLString());
  }
  
  private SubQuery createSubQuery() throws Exception {
    SubQuery subQuery = QueryBuilderFactory.getInstance().newSubQuery();
    Expression<String> varcharColumnA = predicateBuilder.column("A", String.class);
    DBMSTable dbmsTable = QueryBuilderFactory.getInstance().getDBMSTableObject("TestTable");
    Expression<String> outsideColumnB = predicateBuilder.column("out", "B", String.class);
    Predicate wherePredicate = predicateBuilder.equal(varcharColumnA, outsideColumnB);
    
    subQuery.select(varcharColumnA);
    subQuery.from(dbmsTable);
    subQuery.where(wherePredicate);
    
    return subQuery;
  }
  
  @Test
  public void testExistsPredicate() throws Exception {
    Predicate existsPredicate = null;
    
    existsPredicate = predicateBuilder.exists(createSubQuery());
    assertNotNull(existsPredicate);
    assertEquals("EXISTS (SELECT A FROM TestTable WHERE A = out.B)", existsPredicate.toSQLString());
  }
  
  @Test
  public void testNotExistsPredicate() throws Exception {
    Predicate notExistsPredicate = null;
    
    notExistsPredicate = predicateBuilder.notExists(createSubQuery());
    assertNotNull(notExistsPredicate);
    assertEquals("NOT EXISTS (SELECT A FROM TestTable WHERE A = out.B)", notExistsPredicate.toSQLString());
  }
}
