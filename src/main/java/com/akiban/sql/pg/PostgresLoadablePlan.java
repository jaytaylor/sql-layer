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

package com.akiban.sql.pg;

import com.akiban.sql.server.ServerCallExplainer;
import com.akiban.sql.server.ServerCallInvocation;

import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.loadableplan.LoadableOperator;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Explainable;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.List;

public class PostgresLoadablePlan
{
    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerCallInvocation invocation) {
        LoadablePlan<?> loadablePlan = 
            server.getRoutineLoader().loadLoadablePlan(server.getSession(),
                                                       invocation.getRoutineName());
        List<String> columnNames = loadablePlan.columnNames();
        List<PostgresType> columnTypes = columnTypes(loadablePlan);
        boolean usesPValues = server.getBooleanProperty("newtypes", Types3Switch.ON);
        if (loadablePlan instanceof LoadableOperator)
            return new PostgresLoadableOperator((LoadableOperator)loadablePlan, 
                                                invocation,
                                                columnNames, columnTypes,
                                                null, usesPValues);
        if (loadablePlan instanceof LoadableDirectObjectPlan)
            return new PostgresLoadableDirectObjectPlan((LoadableDirectObjectPlan)loadablePlan, 
                                                        invocation,
                                                        columnNames, columnTypes,
                                                        null, usesPValues);
        return null;
    }

    public static PostgresQueryContext setParameters(PostgresQueryContext context, ServerCallInvocation invocation, boolean usePVals) {
        if (!invocation.parametersInOrder()) {
            if (invocation.hasParameters()) {
                PostgresQueryContext calleeContext = 
                    new PostgresQueryContext(context.getServer());
                invocation.copyParameters(context, calleeContext, usePVals);
                context = calleeContext;
            }
            else {
                invocation.copyParameters(null, context, usePVals);
            }
        }
        return context;
    }

    public static List<PostgresType> columnTypes(LoadablePlan<?> plan)
    {
        List<PostgresType> columnTypes = new ArrayList<>();
        for (int jdbcType : plan.jdbcTypes()) {
            DataTypeDescriptor sqlType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType);
            AkType akType = TypesTranslation.sqlTypeToAkType(sqlType);
            TInstance tInstance = TypesTranslation.toTInstance(sqlType);
            columnTypes.add(PostgresType.fromDerby(sqlType, akType, tInstance));
        }
        return columnTypes;
    }

    public static Explainable explainable(PostgresServerSession server, 
                                          final ServerCallInvocation invocation) {
        final LoadablePlan<?> loadablePlan = 
            server.getRoutineLoader().loadLoadablePlan(server.getSession(),
                                                       invocation.getRoutineName());
        return new Explainable() {
                @Override
                public CompoundExplainer getExplainer(ExplainContext context) {
                    Attributes atts = new Attributes();
                    atts.put(Label.PROCEDURE_IMPLEMENTATION, 
                             PrimitiveExplainer.getInstance(loadablePlan.getClass().getName()));
                    return new ServerCallExplainer(invocation, atts, context);
                }
            };
    }

    // All static methods.
    private PostgresLoadablePlan() {
    }
}
