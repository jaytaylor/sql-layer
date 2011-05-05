/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.service.memcache.hprocessor.Scanrows;
import com.akiban.server.service.session.Session;

import java.util.*;
import java.io.IOException;

/**
 * An SQL SELECT transformed into a Hapi request.
 * @see PostgresHapiCompiler
 */
public class PostgresHapiRequest extends PostgresStatement implements HapiGetRequest
{
  private UserTable shallowestTable, queryTable, deepestTable;
  private List<HapiPredicate> predicates; // All on queryTable.

  public PostgresHapiRequest(UserTable shallowestTable, UserTable queryTable, 
                             UserTable deepestTable,
                             List<HapiPredicate> predicates, List<Column> columns) {
    super(columns);
    this.shallowestTable = shallowestTable;
    this.queryTable = queryTable;
    this.deepestTable = deepestTable;
    this.predicates = predicates;
  }

  public UserTable getShallowestTable() {
    return shallowestTable;
  }
  public UserTable getQueryTable() {
    return queryTable;
  }
  public UserTable getDeepestTable() {
    return deepestTable;
  }

  /*** HapiGetRequest ***/

  /**
   * The name of the schema containing the tables involved in this request. Matches getUsingTable().getSchemaName().
   * @return The name of the schema containing the tables involved in this request.
   */
  public String getSchema() {
    return shallowestTable.getName().getSchemaName();
  }
  
  /**
   * Rootmost table to be retrieved by this request.
   * @return The name (without schema) of the rootmost table to be retrieved.
   */
  public String getTable() {
    return shallowestTable.getName().getTableName();
  }

  /**
   * The table whose columns are restricted by this request.
   * @return The schema and table name of the table whose columns are restricted by this request.
   */
  public TableName getUsingTable() {
    return queryTable.getName();
  }

  public int getLimit() {
    return -1;
  }

  public List<HapiPredicate> getPredicates() {
    return predicates;
  }

  /** Only needed in the case where a statement has parameters or the client
   * specifies that some results should be in binary. */
  static class BoundRequest extends PostgresHapiRequest {
    private boolean[] columnBinary; // Is this column binary format?
    private boolean defaultColumnBinary;

    public BoundRequest(UserTable shallowestTable, UserTable queryTable, 
                        UserTable deepestTable,
                        List<HapiPredicate> predicates, List<Column> columns, 
                        boolean[] columnBinary, boolean defaultColumnBinary) {
      super(shallowestTable, queryTable, deepestTable, predicates, columns);
      this.columnBinary = columnBinary;
      this.defaultColumnBinary = defaultColumnBinary;
    }

    public boolean isColumnBinary(int i) {
      if ((columnBinary != null) && (i < columnBinary.length))
        return columnBinary[i];
      else
        return defaultColumnBinary;
    }
  }

  /** Get a bound version of a predicate by applying given parameters
   * and requested result formats. */
  @Override
  public PostgresStatement getBoundRequest(String[] parameters,
                                           boolean[] columnBinary, 
                                           boolean defaultColumnBinary) {
    if ((parameters == null) && 
        (columnBinary == null) && (defaultColumnBinary == false))
      return this;    // Can be reused.

    List<HapiPredicate> unboundPredicates, boundPredicates;
    unboundPredicates = getPredicates();
    boundPredicates = unboundPredicates;
    if (parameters != null) {
      boundPredicates = new ArrayList<HapiPredicate>(unboundPredicates);
      for (int i = 0; i < boundPredicates.size(); i++) {
        PostgresHapiPredicate pred = (PostgresHapiPredicate)boundPredicates.get(i);
        if (pred.getParameterIndex() >= 0) {
          pred = new PostgresHapiPredicate(pred, parameters[pred.getParameterIndex()]);
          boundPredicates.set(i, pred);
        }
      }
    }
    return new BoundRequest(getShallowestTable(), getQueryTable(), getDeepestTable(), 
                            boundPredicates, getColumns(), 
                            columnBinary, defaultColumnBinary);
  }

  @Override
  public int execute(PostgresMessenger messenger, Session session, int maxrows) 
      throws IOException, StandardException {
    PostgresHapiOutputter outputter = new PostgresHapiOutputter(messenger, session,
                                                                this, maxrows);
    // null as OutputStream, since we use the higher level messenger.
    try {
      Scanrows.instance().processRequest(session, this, outputter, null);
    }
    catch (HapiRequestException ex) {
      throw new StandardException(ex);
    }
    return outputter.getNRows();
  }

}
