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
        List<PostgresType> columnTypes = new ArrayList<PostgresType>();
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
