/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserContext;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.Visitable;
import com.foundationdb.sql.parser.Visitor;
import com.foundationdb.sql.views.ViewDefinition;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.TableName;

import com.foundationdb.server.error.SQLParserInternalException;

import java.util.*;

public class AISViewDefinition extends ViewDefinition
{
    private Map<TableName,Collection<String>> tableColumnReferences;

    public AISViewDefinition(String sql, SQLParser parser)
            throws StandardException {
        super(sql, parser);
    }    

    public AISViewDefinition(StatementNode parsed, SQLParserContext parserContext)
            throws StandardException {
        super(parsed, parserContext);
    }
    
    public Map<TableName,Collection<String>> getTableColumnReferences() {
        if (tableColumnReferences == null) {
            ReferenceCollector collector = new ReferenceCollector();
            try {
                getSubquery().accept(collector);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            tableColumnReferences = collector.references;
        }
        return tableColumnReferences;
    }

    static class ReferenceCollector implements Visitor {
        Map<TableName,Collection<String>> references = new HashMap<>();
        
        @Override
        public Visitable visit(Visitable node) throws StandardException {
            if (node instanceof FromTable) {
                TableBinding tableBinding = (TableBinding)((FromTable)node).getUserData();
                if (tableBinding != null) {
                    Columnar table = tableBinding.getTable();
                    if (!references.containsKey(table.getName())) {
                        references.put(table.getName(), new HashSet<String>());
                    }
                }
            }
            else if (node instanceof ColumnReference) {
                ColumnBinding columnBinding = (ColumnBinding)((ColumnReference)node).getUserData();
                if (columnBinding != null) {
                    Column column = columnBinding.getColumn();
                    if (column != null) {
                        Columnar table = column.getColumnar();
                        Collection<String> entry = references.get(table.getName());
                        if (entry == null) {
                            entry = new HashSet<>();
                            references.put(table.getName(), entry);
                        }
                        entry.add(column.getName());
                    }
                }
            }
            return node;
        }

        @Override
        public boolean visitChildrenFirst(Visitable node) {
            return true;
        }

        @Override
        public boolean stopTraversal() {
            return false;
        }

        @Override
        public boolean skipChildren(Visitable node) throws StandardException {
            return false;
        }
    }
}
