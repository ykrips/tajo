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

import java.util.HashMap;
import java.util.Map;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.predicates.derby.DerbyPredicateBuilderImpl;
import org.apache.tajo.catalog.predicates.mariadb.MariaDBPredicateBuilderImpl;
import org.apache.tajo.catalog.predicates.mysql.MySQLPredicateBuilderImpl;
import org.apache.tajo.catalog.predicates.oracle.OraclePredicateBuilderImpl;
import org.apache.tajo.catalog.predicates.postgresql.PostgreSQLPredicateBuilderImpl;
import org.apache.tajo.catalog.predicates.spi.DBMSTableImpl;
import org.apache.tajo.catalog.predicates.spi.AbstractQueryImpl;
import org.apache.tajo.catalog.predicates.spi.SubQueryImpl;

/**
 * Factory class which is responsible for creating query, subquery, predicate builder, and dbms table object.
 */
public class QueryBuilderFactory {

  public static enum DBMSType {
    Derby,
    MySQL,
    MariaDB,
    PostgreSQL,
    Oracle
  }
  
  private static final QueryBuilderFactory instance = new QueryBuilderFactory();
  
  public static QueryBuilderFactory getInstance() {
    return instance;
  }
  
  private final Map<DBMSType, PredicateBuilder> predicateBuilderMap;
  
  private QueryBuilderFactory() {
    predicateBuilderMap = 
        new HashMap<QueryBuilderFactory.DBMSType, PredicateBuilder>(DBMSType.values().length);
    predicateBuilderMap.put(DBMSType.Derby, new DerbyPredicateBuilderImpl());
    predicateBuilderMap.put(DBMSType.MySQL, new MySQLPredicateBuilderImpl());
    predicateBuilderMap.put(DBMSType.MariaDB, new MariaDBPredicateBuilderImpl());
    predicateBuilderMap.put(DBMSType.PostgreSQL, new PostgreSQLPredicateBuilderImpl());
    predicateBuilderMap.put(DBMSType.Oracle, new OraclePredicateBuilderImpl());
  }
  
  public Query newQuery() {
    return new AbstractQueryImpl();
  }
  
  public SubQuery newSubQuery() {
    return new SubQueryImpl();
  }
  
  public PredicateBuilder getPredicateBuilder(DBMSType supportedDBMS) {
    PredicateBuilder builder = predicateBuilderMap.get(supportedDBMS);
    
    if (builder == null) {
      throw new UnsupportedOperationException(supportedDBMS + " is not supported yet.");
    }
    
    return builder;
  }
  
  public DBMSTable getDBMSTableObject(String canonicalName) {
    if (canonicalName == null || canonicalName.isEmpty()) {
      throw new IllegalArgumentException("Canonical Name is null or empty.");
    }
    
    DBMSTable dbmsTable;
    
    if (CatalogUtil.isFQTableName(canonicalName)) {
      String[] splittedTableNames = CatalogUtil.splitTableName(canonicalName);
      dbmsTable = new DBMSTableImpl(splittedTableNames[0], splittedTableNames[1]);
    } else {
      dbmsTable = new DBMSTableImpl("", canonicalName);
    }
    
    return dbmsTable;
  }
}
