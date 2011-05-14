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
import com.akiban.server.service.session.Session;

import java.util.*;
import java.io.IOException;

/**
 * An SQL statement compiled for use with Postgres server.
 * @see PostgresStatementCompiler
 */
public abstract class PostgresStatement
{
  private List<Column> columns;
  private List<PostgresType> types;

  protected PostgresStatement() {
    this.columns = null;
  }

  protected PostgresStatement(List<Column> columns) {
    this.columns = columns;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public boolean isColumnBinary(int i) {
    return false;
  }

  public List<PostgresType> getTypes() throws StandardException {
    if (types == null) {
      if (columns == null) return null;
      types = new ArrayList<PostgresType>(columns.size());
      for (Column column : columns) {
        types.add(PostgresType.fromAIS(column));
      }
    }
    return types;
  }

  public void sendDescription(PostgresServerSession server) 
      throws IOException, StandardException {
    PostgresMessenger messenger = server.getMessenger();
    List<Column> columns = getColumns();
    if (columns == null) {
      messenger.beginMessage(PostgresMessenger.NO_DATA_TYPE);
    }
    else {
      messenger.beginMessage(PostgresMessenger.ROW_DESCRIPTION_TYPE);
      List<PostgresType> types = getTypes();
      int ncols = columns.size();
      messenger.writeShort(ncols);
      for (int i = 0; i < ncols; i++) {
        Column col = columns.get(i);
        PostgresType type = types.get(i);
        messenger.writeString(col.getName()); // attname
        messenger.writeInt(0);    // attrelid
        messenger.writeShort(0);  // attnum
        messenger.writeInt(type.getOid()); // atttypid
        messenger.writeShort(type.getLength()); // attlen
        messenger.writeInt(type.getModifier()); // atttypmod
        messenger.writeShort(isColumnBinary(i) ? 1 : 0);
      }
    }
    messenger.sendMessage();
  }
  
  public PostgresStatement getBoundRequest(String[] parameters,
                                           boolean[] columnBinary, 
                                           boolean defaultColumnBinary) {
    return this;
  }

  public abstract void execute(PostgresServerSession server, int maxrows)
      throws IOException, StandardException;

}
