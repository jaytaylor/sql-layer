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

package com.foundationdb.server.service.restdml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.optimizer.rule.PlanGenerator;

public class InsertGenerator extends OperatorGenerator {

    private Table table;
    
    public InsertGenerator (AkibanInformationSchema ais) {
        super(ais);
    }

    protected Operator create(TableName tableName) {
        return create(Collections.<Column, String>emptyMap(), tableName);
    }

    protected Operator create(Map<Column, String> values, TableName tableName) {
        table = ais().getTable(tableName);
        RowStream stream = new RowStream();
        List<TPreparedExpression> inputExprs = assembleValueScan(stream, values.values());
        stream = assemptInsertProject(stream, values.keySet(), inputExprs, table);
        stream.operator = API.insert_Returning(stream.operator);
        stream = assembleReturningProject(stream, table);
        return stream.operator; 
    }
    
    protected RowStream assemptInsertProject(RowStream input,
                                             Collection<Column> inputColumns,
                                             List<TPreparedExpression> inputExprs,
                                             Table table) {
        assert inputColumns.size() == inputExprs.size();
        assert input.rowType.nFields() == inputExprs.size();
        List<TPreparedExpression> insertExprs = new ArrayList<>(inputColumns.size());
        for (int i = 0; i < inputColumns.size(); ++i) {
            insertExprs.add(new TPreparedField(input.rowType.typeAt(i), i));
        }
        // Fill in input values
        Iterator<Column> colIt = inputColumns.iterator();
        TableRowType targetRowType = schema().tableRowType(table);
        TPreparedExpression[] row = new TPreparedExpression[targetRowType.nFields()];
        for (int i = 0; i < inputColumns.size(); i++) {
            Column column = colIt.next();
            TInstance type = column.getType();
            int pos = column.getPosition();
            row[pos] = insertExprs.get(i);
            if(!type.equals(row[pos].resultType())) {
                TCast tcast = registryService().getCastsResolver().cast(row[pos].resultType().typeClass(),
                                                                        type.typeClass());
                row[pos] = new TCastExpression(row[pos], tcast, type);
            }
        }
        // Fill in column defaults
        for(int i = 0, len = targetRowType.nFields(); i < len; ++i) {
            Column column = table.getColumnsIncludingInternal().get(i);
            row[i] = PlanGenerator.generateDefaultExpression(column,
                                                             row[i],
                                                             registryService(),
                                                             getTypesTranslator(),
                                                             queryContext());
        }
        // Now a complete row
        insertExprs = Arrays.asList(row);
        input.operator = API.project_Table(input.operator, input.rowType, targetRowType, insertExprs);
        input.rowType = targetRowType;
        return input;
    }

    protected List<TPreparedExpression> assembleValueScan(RowStream stream, Collection<String> inputValues) {
        TInstance[] insts = new TInstance[inputValues.size()];
        List<TPreparedExpression> exprs = new ArrayList<>(inputValues.size());
        int i = 0;
        for(String v : inputValues) {
            insts[i] = getTypesTranslator().typeForString(v);
            exprs.add(new TPreparedLiteral(insts[i], ValueSources.valuefromObject(v, insts[i])));
            ++i;
        }
        stream.rowType = schema().newValuesType(insts);
        List<BindableRow> bindableRows = Arrays.asList(BindableRow.of(stream.rowType, exprs, queryContext()));
        stream.operator = API.valuesScan_Default(bindableRows, stream.rowType);
        return exprs;
    }

    protected RowStream assembleReturningProject(RowStream stream, Table table) {
        if(table.getPrimaryKey() != null) {
            PrimaryKey key = table.getPrimaryKey();
            int size  = key.getIndex().getKeyColumns().size();
            List<TPreparedExpression> pExpressions = new ArrayList<>(size);
            for(IndexColumn column : key.getIndex().getKeyColumns()) {
                int fieldIndex = column.getColumn().getPosition();
                pExpressions.add(new TPreparedField(stream.rowType.typeAt(fieldIndex), fieldIndex));
            }
            stream.operator = API.project_Table(stream.operator,
                                                stream.rowType,
                                                schema().tableRowType(table),
                                                pExpressions);
        }
        return stream;
    }
}
