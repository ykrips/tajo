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

import org.apache.tajo.catalog.predicates.DBMSTable;

/**
 * It represents a table on DBMS.
 */
public class DBMSTableImpl implements DBMSTable {

  private String schemaName;
  
  private String tableName;
  
  public DBMSTableImpl(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public String getSchemaName() {
    return schemaName;
  }

  @Override
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
}
