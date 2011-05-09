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

import com.akiban.sql.optimizer.OperatorCompiler;

import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.CursorNode;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;

import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitGroupRow;

import com.akiban.qp.row.Row;

import com.akiban.server.api.dml.scan.NiceRow;

import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Compile SQL SELECT statements into operator trees if possible.
 */
public class PostgresOperatorCompiler extends OperatorCompiler
                                      implements PostgresStatementCompiler
{
  private static final Logger logger = LoggerFactory.getLogger(PostgresOperatorCompiler.class);

  private PersistitAdapter adapter;

  public PostgresOperatorCompiler(SQLParser parser, 
                                  AkibanInformationSchema ais, String defaultSchemaName,
                                  Session session, ServiceManager serviceManager) {
    super(parser, ais, defaultSchemaName);
    PersistitStore persistitStore = ((OperatorStore)
                                     serviceManager.getStore()).getPersistitStore();
    adapter = new PersistitAdapter(schema, persistitStore, session);
  }

  @Override
  public PostgresStatement compile(CursorNode cursor, int[] paramTypes)
      throws StandardException {
    Result result = compile(cursor);

    logger.warn("Operator:\n{}", result);

    return new PostgresOperatorStatement(adapter, 
                                         result.getResultOperator(),
                                         result.getResultRowType(),
                                         result.getResultColumns(),
                                         result.getResultColumnOffsets());
  }

  protected Row getIndexRow(Index index, Object[] keys) {
    NiceRow niceRow = new NiceRow(index.getTable().getTableId());
    for (int i = 0; i < keys.length; i++) {
      niceRow.put(index.getColumns().get(i).getColumn().getPosition(), keys[i]);
    }
    return PersistitGroupRow.newPersistitGroupRow(adapter, niceRow.toRowData());
  }

}
