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
package com.akiban.server.service.restdml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedParameter;
import com.akiban.sql.optimizer.rule.PlanGenerator;

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
        
        int nfields = table.getColumns().size();
        TInstance[] types = new TInstance[nfields];
        TInstance varchar = Column.generateTInstance(null, Types.VARCHAR, 65535L, null, false);
        List<TPreparedExpression> tExprs = new ArrayList<>();
        for (int index = 0; index < table.getColumns().size(); index++) {
            tExprs.add(index, new TPreparedParameter(index, varchar));
            types[index] = varchar;
        }
        stream.rowType =  schema().newValuesType(types);
        bindableRows.add(BindableRow.of(stream.rowType, null, tExprs, queryContext()));
        stream.operator = API.valuesScan_Default(bindableRows, stream.rowType);
        return stream;
    }
}
