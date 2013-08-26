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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.PrimaryKey;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Types;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.t3expressions.T3RegistryService;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;
import com.foundationdb.server.types3.texpressions.TPreparedField;
import com.foundationdb.server.types3.texpressions.TPreparedParameter;
import com.foundationdb.sql.optimizer.rule.PlanGenerator;

public abstract class OperatorGenerator {
    
    private Schema schema;
    private AkibanInformationSchema ais;
    private QueryContext queryContext;
    private T3RegistryService registryService;
    
    private Map<TableName,Operator> plans = new HashMap<>();
    
    protected abstract Operator create (TableName tableName); 
    
    public OperatorGenerator (AkibanInformationSchema ais) {
        this.ais = ais;
        this.schema = SchemaCache.globalSchema(ais);
        queryContext = new SimpleQueryContext(null);
    }
    
    public void setT3Registry(T3RegistryService registryService) {
        this.registryService = registryService;
    }
    
    public Operator get (TableName tableName) {
        Operator plan = null;
        if (plans.containsKey(tableName)) {
            plan = plans.get(tableName);
        } else {
            plan = create (tableName);
            plans.put(tableName, plan);
        }
        return plan;
    }
    
    public Schema schema() { 
        return schema;
    }
    
    public AkibanInformationSchema ais() {
        return ais;
    }
    
    public QueryContext queryContext() {
        return queryContext;
    }
    
    public T3RegistryService registryService() {
        return registryService;
    }

    static class RowStream {
        Operator operator;
        RowType rowType;
    }
    
    protected Operator indexAncestorLookup(TableName tableName) {
        UserTable table = ais().getUserTable(tableName);
        return PlanGenerator.generateAncestorPlan(ais(), table);
    }

    protected RowStream assembleValueScan(UserTable table) {
        RowStream stream = new RowStream();
        List<BindableRow> bindableRows = new ArrayList<>();
        List<TPreparedExpression> tExprs = parameters (table);
        
        TInstance varchar = Column.generateTInstance(null, Types.VARCHAR, 65535L, null, false);
        int nfields = table.getColumns().size();
        TInstance[] types = new TInstance[nfields];
        for (int index = 0; index < table.getColumns().size(); index++) {
            types[index] = varchar;
        }

        stream.rowType =  schema().newValuesType(types);
        bindableRows.add(BindableRow.of(stream.rowType, tExprs, queryContext()));
        stream.operator = API.valuesScan_Default(bindableRows, stream.rowType);
        return stream;
    }
    
    protected List<TPreparedExpression> parameters (UserTable table) {
        TInstance varchar = Column.generateTInstance(null, Types.VARCHAR, 65535L, null, false);
        List<TPreparedExpression> tExprs = new ArrayList<>();
        for (int index = 0; index < table.getColumns().size(); index++) {
            tExprs.add(index, new TPreparedParameter(index, varchar));
        }
        return tExprs;
    }
    
    protected RowStream projectTable (RowStream stream, UserTable table) {
        List<TPreparedExpression> pExpressions = null;
        if (table.getPrimaryKey() != null) {
            PrimaryKey key = table.getPrimaryKey();
            int size  = key.getIndex().getKeyColumns().size();
            pExpressions = new ArrayList<>(size);
            for (IndexColumn column : key.getIndex().getKeyColumns()) {
                int fieldIndex = column.getColumn().getPosition();
                pExpressions.add (new TPreparedField(stream.rowType.typeInstanceAt(fieldIndex), fieldIndex));
            }
            stream.operator = API.project_Table(stream.operator,
                    stream.rowType,
                    schema().userTableRowType(table),
                    pExpressions);
        }
        return stream;
    }
}
