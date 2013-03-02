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
import com.akiban.ais.model.FullTextIndex;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchTableException;

import org.apache.lucene.analysis.Analyzer;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class FullTextIndexInfo
{
    private final FullTextIndexShared shared;
    private FullTextIndex index;
    private Schema schema;
    private UserTableRowType indexedRowType;
    private HKeyRowType hKeyRowType;
    private Map<Column,IndexedField> fieldsByColumn;
    private Map<RowType,List<IndexedField>> fieldsByRowType;
    private String defaultFieldName;

    public FullTextIndexInfo(FullTextIndexShared shared) {
        this.shared = shared;
    }

    public void init(AkibanInformationSchema ais) {
        IndexName name = shared.getName();
        UserTable table = ais.getUserTable(name.getFullTableName());
        if (table == null) {
            throw new NoSuchTableException(name.getFullTableName());
        }
        index = table.getFullTextIndex(name.getName());
        if (index == null) {
            throw new NoSuchIndexException(name.getName());
        }
        schema = SchemaCache.globalSchema(ais);
        indexedRowType = schema.userTableRowType(table);
        hKeyRowType = schema.newHKeyRowType(table.hKey());
        fieldsByColumn = new HashMap<>(index.getKeyColumns().size());
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            Column column = indexColumn.getColumn();
            IndexedField indexedField = new IndexedField(column);
            fieldsByColumn.put(column, indexedField);
            if (defaultFieldName == null) {
                defaultFieldName = indexedField.getName();
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

    public FullTextIndex getIndex() {
        return index;
    }

    public Schema getSchema() {
        return schema;
    }

    public UserTableRowType getIndexedRowType() {
        return indexedRowType;
    }

    public HKeyRowType getHKeyRowType() {
        return hKeyRowType;
    }

    public Map<Column,IndexedField> getFieldsByColumn() {
        return fieldsByColumn;
    }

    public Map<RowType,List<IndexedField>> getFieldsByRowType() {
        return fieldsByRowType;
    }

    public Set<String> getCasePreservingFieldNames() {
        Set<String> result = new HashSet<>();
        for (IndexedField field : fieldsByColumn.values()) {
            if (field.isCasePreserving()) {
                result.add(field.getName());
            }
        }
        return result;
    }

    public String getDefaultFieldName() {
        return defaultFieldName;
    }

    public Set<RowType> getRowTypes() {
        Set<RowType> rowTypes = new HashSet<>(fieldsByRowType.keySet());
        rowTypes.add(indexedRowType);
        return rowTypes;
    }

    public Operator fullScan() {
        Operator plan = API.groupScan_Default(indexedRowType.userTable().getGroup());
        Set<RowType> rowTypes = getRowTypes();
        plan = API.filter_Default(plan, rowTypes);
        return plan;
    }

    public Analyzer getAnalyzer() {
        Analyzer analyzer;
        synchronized (shared) {
            analyzer = shared.getAnalyzer();
            if (analyzer == null) {
                analyzer = new SelectiveCaseAnalyzer(shared.getCasePreservingFieldNames());
            }
        }
        return analyzer;
    }

    protected Searcher getSearcher() throws IOException {
        Searcher searcher;
        synchronized (shared) {
            searcher = shared.getSearcher();
            if (searcher == null) {
                searcher = new Searcher(shared, getAnalyzer());
            }
            shared.setSearcher(searcher);
        }
        return searcher;
    }

    public Indexer getIndexer() throws IOException {
        Indexer indexer;
        synchronized (shared) {
            indexer = shared.getIndexer();
            if (indexer == null) {
                indexer = new Indexer(shared, getAnalyzer());
                shared.setIndexer(indexer);
            }
        }
        return indexer;
    }

    // TODO: _Lookup plan to get rows just from a row being updated.

    public void deletePath() {
        File path = shared.getPath();
        for (File f : path.listFiles()) {
            f.delete();
        }
        path.delete();
    }

}
