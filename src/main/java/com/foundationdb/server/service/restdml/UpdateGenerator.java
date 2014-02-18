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

import static com.foundationdb.util.Strings.join;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedParameter;

public class UpdateGenerator extends OperatorGenerator {

    private Table table;
    private static final Logger logger = LoggerFactory.getLogger(UpdateGenerator.class);

    public UpdateGenerator(AkibanInformationSchema ais) {
        super(ais);
    }

    protected Operator create(TableName tableName, List<Column> upColumns) {
        table = ais().getTable(tableName);
        RowStream stream = new RowStream ();
        stream.operator = indexAncestorLookup(tableName); 
        stream.rowType = schema().tableRowType(table);

        TInstance varchar = getTypesTranslator().typeForString();
        TPreparedExpression[] updates = new TPreparedExpression[table.getColumns().size()];

        // The Primary Key columns have already been added as query parameters
        // by the indexAncestorLookup ($1,,) So start the new parameters from there
        // And we don't want to add the PK columns as to be updated. 
        List<Column> pkList = table.getPrimaryKey().getColumns();
        int paramIndex = pkList.size();
        int index = 0;
        for (Column column : table.getColumns()) {
            if (!pkList.contains(column) && upColumns.contains(column)) {
                updates[index] =  new TPreparedParameter(paramIndex, varchar);
                
                if (!column.getType().equals(varchar)) {
                    TCast cast = registryService().getCastsResolver().cast(varchar.typeClass(),
                            column.getType().typeClass());
                    updates[index] = new TCastExpression(updates[index], cast, column.getType());
                }
                paramIndex++;
            }
            index++;
        }
        UpdateFunction updateFunction = 
                new UpsertRowUpdateFunction(Arrays.asList(updates), stream.rowType);
        stream.operator = API.update_Returning(stream.operator, updateFunction);
        
        if (logger.isDebugEnabled()) {
            ExplainContext explain = explainUpdateStatement(stream.operator, table, Arrays.asList(updates));
            DefaultFormatter formatter = new DefaultFormatter(table.getName().getSchemaName());
            logger.debug("Update Plan for {}:\n{}", table,
                         join(formatter.format(stream.operator.getExplainer(explain))));
        }
        return stream.operator;
    }

    
    protected ExplainContext explainUpdateStatement(Operator plan, Table table, List<TPreparedExpression> updatesP) {
        
        ExplainContext explainContext = new ExplainContext();        
        Attributes atts = new Attributes();
        atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(table.getName().getSchemaName()));
        atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(table.getName().getTableName()));
        for (Column column : table.getColumns()) {
            if (updatesP.get(column.getPosition()) != null) {
                atts.put(Label.COLUMN_NAME, PrimitiveExplainer.getInstance(column.getName()));
                atts.put(Label.EXPRESSIONS, updatesP.get(column.getPosition()).getExplainer(explainContext));
            }
        }
        explainContext.putExtraInfo(plan, new CompoundExplainer(Type.EXTRA_INFO, atts));
        return explainContext;
    }
}
