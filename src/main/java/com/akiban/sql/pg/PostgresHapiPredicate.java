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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiPredicate;

public class PostgresHapiPredicate implements HapiPredicate
{
  private Column column;
  private Operator op;
  private String value;
  private int parameterIndex;

  public PostgresHapiPredicate(Column column, Operator op, String value) {
    this.column = column;
    this.op = op;
    this.value = value;
    this.parameterIndex = -1;
  }

  public PostgresHapiPredicate(Column column,
                               Operator op, int parameterIndex) {
    this.column = column;
    this.op = op;
    this.parameterIndex = parameterIndex;
  }

  public PostgresHapiPredicate(PostgresHapiPredicate other, String value) {
    this.column = other.column;
    this.op = other.op;
    this.value = value;
    this.parameterIndex = -1;
  }
                               
  public TableName getTableName() {
    return column.getUserTable().getName();
  }

  public String getColumnName() {
    return column.getName();
  }

  public Operator getOp() {
    return op;
  }

  public String getValue() {
    return value;
  }

  public int getParameterIndex() {
    return parameterIndex;
  }
}
