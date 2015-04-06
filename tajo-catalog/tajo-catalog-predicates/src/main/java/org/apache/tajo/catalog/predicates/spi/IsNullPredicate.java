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