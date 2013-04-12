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

import static com.akiban.util.Strings.join;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.format.DefaultFormatter;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TCastExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedParameter;

public class UpdateGenerator extends OperatorGenerator {

    private UserTable table;
    private static final Logger logger = LoggerFactory.getLogger(UpdateGenerator.class);

    public UpdateGenerator(AkibanInformationSchema ais) {
        super(ais);
    }

    
    @Override
    protected Operator create(TableName tableName) {
        table = ais().getUserTable(tableName);

        return create (tableName, table.getColumns());
    }
    
    protected Operator create (TableName tableName, List<Column> upColumns) {
        table = ais().getUserTable(tableName);

        RowStream stream = new RowStream ();
        stream.operator = indexAncestorLookup(tableName); 
        stream.rowType = schema().userTableRowType(table);

        TInstance varchar = Column.generateTInstance(null, Types.VARCHAR, 65535L, null, true);
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
                
                if (!column.tInstance().equals(varchar)) {
                    TCast cast = registryService().getCastsResolver().cast(varchar.typeClass(),
                            column.tInstance().typeClass()); 
                    updates[index] = new TCastExpression(updates[index], cast, column.tInstance(), queryContext());
                }
                paramIndex++;
            }
            index++;
        }
        UpdateFunction updateFunction = 
                new UpsertRowUpdateFunction(Arrays.asList(updates), stream.rowType);
        stream.operator = API.update_Returning(stream.operator, updateFunction, true);
        
        if (logger.isDebugEnabled()) {
            ExplainContext explain = explainUpdateStatement(stream.operator, table, Arrays.asList(updates));
            DefaultFormatter formatter = new DefaultFormatter(table.getName().getSchemaName());
            logger.debug("Update Plan for {}:\n{}", table,
                         join(formatter.format(stream.operator.getExplainer(explain))));
        }
        return stream.operator;
    }

    
    protected ExplainContext explainUpdateStatement(Operator plan, UserTable table, List<TPreparedExpression> updatesP) {
        
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
