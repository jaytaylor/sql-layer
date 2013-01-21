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

package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.IndexTableNotInGroupException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;

import java.util.*;

public class FullTextIndexAIS
{
    private final FullTextIndex index;
    private final AkibanInformationSchema ais;
    private UserTableRowType indexedRowType;
    private List<IndexedField> keyFields;
    private Map<Column,IndexedField> fieldsByColumn;
    private Map<RowType,List<IndexedField>> fieldsByRowType;

    public FullTextIndexAIS(FullTextIndex index, AkibanInformationSchema ais) {
        this.index = index;
        this.ais = ais;
    }

    public void init() {
        Schema schema = SchemaCache.globalSchema(ais);
        UserTable table = ais.getUserTable(index.getSchemaName(), index.getTableName());
        if (table == null) {
            throw new NoSuchTableException(index.getSchemaName(), index.getTableName());
        }
        indexedRowType = schema.userTableRowType(table);
        List<Column> pkCols = table.getPrimaryKeyIncludingInternal().getColumns();
        keyFields = new ArrayList<>(pkCols.size());
        fieldsByColumn = new HashMap<>(pkCols.size() + index.getIndexedColumns().size());
        for (Column pkCol : pkCols) {
            IndexedField field = new IndexedField(pkCol, true);
            keyFields.add(field);
            fieldsByColumn.put(pkCol, field);
        }
        for (String cstr : index.getIndexedColumns()) {
            Column col;
            int idx = cstr.lastIndexOf('.');
            if (idx < 0) {
                col = table.getColumn(cstr);
                if (col == null) {
                    throw new NoSuchColumnException(cstr);
                }
            }
            else {
                String tstr = cstr.substring(0, idx);
                cstr = cstr.substring(idx+1);
                String schemaName, tableName;
                idx = tstr.indexOf('.');
                if (idx < 0) {
                    schemaName = index.getSchemaName();
                    tableName = tstr;
                }
                else {
                    schemaName = tstr.substring(0, idx);
                    tableName = tstr.substring(idx+1);
                }
                UserTable otherTable = ais.getUserTable(schemaName, tableName);
                if (otherTable == null) {
                    throw new NoSuchTableException(schemaName, tableName);
                }
                if (otherTable.getGroup() != table.getGroup()) {
                    throw new IndexTableNotInGroupException(index.getName(), cstr,
                                                            tableName);
                }
                col = otherTable.getColumn(cstr);
                if (col == null) {
                    throw new NoSuchColumnException(cstr);
                }
            }
            if (!fieldsByColumn.containsKey(col)) {
                fieldsByColumn.put(col, new IndexedField(col, false));
            }
        }
        fieldsByRowType = new HashMap<>();
        for (Map.Entry<Column,IndexedField> entry : fieldsByColumn.entrySet()) {
            UserTableRowType rowType = schema.userTableRowType(entry.getKey().getUserTable());
            List<IndexedField> fields = fieldsByRowType.get(rowType);
            if (fields == null) {
                fields = new ArrayList<>();
                fieldsByRowType.put(rowType, fields);
            }
            fields.add(entry.getValue());
        }
    }

    public List<String> getKeyColumns() {
        List<String> result = new ArrayList<>(keyFields.size());
        for (IndexedField field : keyFields) {
            result.add(field.getName());
        }
        return result;
    }
}
