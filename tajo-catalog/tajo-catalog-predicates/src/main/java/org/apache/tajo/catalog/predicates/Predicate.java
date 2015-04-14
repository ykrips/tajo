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

/**
 * Predicate defines the following manner.
 * 
 * <pre>
 * &lt;predicate&gt; ::= <br/>
 *     { 
 *     expression { = | &lt; &gt; | ! = | &gt; | &gt; = | ! &gt; | &lt; | &lt; = | ! &lt; } expression
 *     | string_expression [ NOT ] LIKE string_expression
 *         [ ESCAPE 'escape_exression' ]
 *     | expression [ NOT ] BETWEEN expression AND expression
 *     | expression IS [ NOT ] NULL
 *     | expression [ NOT ] IN ( subquery | expression [ ,...n ] ) 
 *     | expression { = | &lt; &gt; | ! = | &gt; | &gt; = | ! &gt; | &lt; | &lt; = | ! &lt; }
 *         { ALL | SOME | ANY} ( subquery ) 
 *     | EXISTS ( subquery )
 *     }
 * </pre>
 */
public interface Predicate extends Expression<Boolean> {

  boolean isNot();
  
  Predicate not();
  
}
