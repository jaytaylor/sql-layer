package com.akiban.server.service.restdml;

import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.t3expressions.T3RegistryService;
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
}
