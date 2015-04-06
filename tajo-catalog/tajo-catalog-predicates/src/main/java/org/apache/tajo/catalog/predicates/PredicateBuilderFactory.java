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
import org.apache.tajo.catalog.predicates.spi.DBMSTableImpl;

/**
 * 
 */
public class PredicateBuilderFactory {

  public static enum DBMSType {
    Derby,
    MySQL,
    MariaDB,
    PostgreSQL,
    Oracle
  }
  
  private static final PredicateBuilderFactory instance =
      new PredicateBuilderFactory();
  
  public static PredicateBuilderFactory newInstance() {
    return instance;
  }
  
  private final Map<DBMSType, PredicateBuilder> builderMap;
  
  private PredicateBuilderFactory() {
    builderMap = new HashMap<PredicateBuilderFactory.DBMSType, PredicateBuilder>(DBMSType.values().length);
  }
  
  public PredicateBuilder getPredicateBuilder(DBMSType supportedDBMS) {
    PredicateBuilder builder = builderMap.get(supportedDBMS);
    
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
    String[] splittedTableNames = CatalogUtil.splitTableName(canonicalName);
    if (CatalogUtil.isFQTableName(canonicalName)) {
      dbmsTable = new DBMSTableImpl(splittedTableNames[0], splittedTableNames[1]);
    } else {
      dbmsTable = new DBMSTableImpl("", splittedTableNames[0]);
    }
    
    return dbmsTable;
  }
}
