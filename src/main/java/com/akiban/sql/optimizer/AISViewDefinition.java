
package com.akiban.sql.optimizer;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.ColumnReference;
import com.akiban.sql.parser.FromTable;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SQLParserContext;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.Visitable;
import com.akiban.sql.parser.Visitor;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.TableName;

import com.akiban.server.error.SQLParserInternalException;

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
