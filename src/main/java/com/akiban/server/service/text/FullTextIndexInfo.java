/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.FullTextIndex;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.HKeyRow;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchTableException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;

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

    private Operator branchLookup_Nested(HKeyRow row)
    {
        Operator plan = null;
        RowType rowType = row.rowType();
        for (IndexColumn ic : index.getAllColumns())
        {
            // if any column in the index def belongs a a table
            // that is a descendant of this row's table
            // (meaning this indexed row has descendants)
            // then do branchlookup_nested
            if (ic.getColumn().getUserTable().isDescendantOf(rowType.userTable()))
            {
                
                plan = API.branchLookup_Nested(rowType.userTable().getGroup(), 
                                               rowType,
                                               schema.userTableRowType(rowType.userTable()),
                                               API.InputPreservationOption.DISCARD_INPUT,
                                               0);
                break;
            }   
        }

        return plan;
    }
    
    /**
     * 
     * @param row
     * @return the operator plan to get to every row related to this index row
     */
    public Operator getOperator(HKeyRow row)
    {
        Operator plan = null;
        
        
        RowType rowType = row.rowType();
        for (IndexColumn ic : index.getAllColumns())
        {
            // if any column in the index def belongs a a table
            // that is a descendant of this row's table
            // (meaning this indexed row has descendants)
            // then do branchlookup_nested
            if (ic.getColumn().getUserTable().isDescendantOf(rowType.userTable()))
            {
                plan = API.branchLookup_Nested(rowType.userTable().getGroup(), 
                                               rowType,
                                               indexedRowType,
                                               API.InputPreservationOption.DISCARD_INPUT,
                                               0);
                
                plan = API.ancestorLookup_Nested(indexedRowType.userTable().getGroup(), 
                                         plan.rowType(),
                                         Arrays.asList(indexedRowType), 
                                         0);
                break;
            }   
        }
        
        if (plan == null) // no descendants
        {
            // Do ancestor_lookup_default
            IndexRowType indexRowType = indexedRowType.indexRowType(index);

            plan = API.indexScan_Default(indexRowType,
                                        IndexKeyRange.unbounded(indexRowType),
                                        new API.Ordering(),
                                        indexedRowType);

            
        }
        
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

    public StandardQueryParser getParser() {
        StandardQueryParser parser;
        synchronized (shared) {
            parser = shared.getParser();
            if (parser == null) {
                parser = new StandardQueryParser(getAnalyzer());
            }
            shared.setParser(parser);
        }
        return parser;
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
