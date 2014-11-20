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
package com.foundationdb.rest.dml;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.optimizer.rule.PlanGenerator;

public abstract class OperatorGenerator {
    
    private Schema schema;
    private AkibanInformationSchema ais;
    private QueryContext queryContext;
    private TypesRegistryService typesRegistry;
    private TypesTranslator typesTranslator;
    
    public OperatorGenerator (AkibanInformationSchema ais) {
        this.ais = ais;
        this.schema = SchemaCache.globalSchema(ais);
        queryContext = new SimpleQueryContext(null);
    }
    
    public void setTypesRegistry(TypesRegistryService registryService) {
        this.typesRegistry = registryService;
    }
    
    public TypesTranslator getTypesTranslator() {
        return typesTranslator;
    }

    public void setTypesTranslator(TypesTranslator typesTranslator) {
        this.typesTranslator = typesTranslator;
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
    
    public TypesRegistryService registryService() {
        return typesRegistry;
    }

    static class RowStream {
        Operator operator;
        RowType rowType;
    }
    
    protected Operator indexAncestorLookup(TableName tableName) {
        Table table = ais().getTable(tableName);
        return PlanGenerator.generateAncestorPlan(ais(), table);
    }
}
