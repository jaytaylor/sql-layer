/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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

import com.akiban.server.error.SQLParserInternalException;

import java.util.*;

public class AISViewDefinition extends ViewDefinition
{
    private Map<Columnar,Collection<Column>> tableColumnReferences;

    public AISViewDefinition(String sql, SQLParser parser)
            throws StandardException {
        super(sql, parser);
    }    

    public AISViewDefinition(StatementNode parsed, SQLParserContext parserContext)
            throws StandardException {
        super(parsed, parserContext);
    }
    
    public Collection<Columnar> getTableReferences() {
        return getTableColumnReferences().keySet();
    }

    public boolean referencesColumn(Column column) {
        Collection<Column> columns = getTableColumnReferences().get(column.getTable());
        return ((columns != null) && columns.contains(column));
    }

    public Map<Columnar,Collection<Column>> getTableColumnReferences() {
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
        Map<Columnar,Collection<Column>> references = new HashMap<Columnar,Collection<Column>>();
        
        @Override
        public Visitable visit(Visitable node) throws StandardException {
            if (node instanceof FromTable) {
                TableBinding tableBinding = (TableBinding)((FromTable)node).getUserData();
                if (tableBinding != null) {
                    Columnar table = tableBinding.getTable();
                    if (!references.containsKey(table)) {
                        references.put(table, new HashSet<Column>());
                    }
                }
            }
            else if (node instanceof ColumnReference) {
                ColumnBinding columnBinding = (ColumnBinding)((ColumnReference)node).getUserData();
                if (columnBinding != null) {
                    Column column = columnBinding.getColumn();
                    if (column != null) {
                        Columnar table = column.getTable();
                        Collection<Column> entry = references.get(table);
                        if (entry == null) {
                            entry = new HashSet<Column>();
                            references.put(table, entry);
                        }
                        entry.add(column);
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
