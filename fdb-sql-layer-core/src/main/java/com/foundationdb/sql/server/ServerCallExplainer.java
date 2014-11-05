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

package com.foundationdb.sql.server;

import com.foundationdb.server.explain.*;

public class ServerCallExplainer extends CompoundExplainer
{
    public ServerCallExplainer(ServerRoutineInvocation invocation, Attributes atts, ExplainContext context) {
        super(Type.PROCEDURE, addAttributes(atts, (ServerCallInvocation)invocation, context));
    }

    private static Attributes addAttributes(Attributes atts, 
                                            ServerCallInvocation invocation,
                                            ExplainContext context) {
        atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(invocation.getRoutineName().getSchemaName()));
        atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(invocation.getRoutineName().getTableName()));
        atts.put(Label.PROCEDURE_CALLING_CONVENTION, PrimitiveExplainer.getInstance(invocation.getCallingConvention().name()));
        for (int i = 0; i < invocation.size(); i++) {
            int paramNumber = invocation.getParameterNumber(i);
            CompoundExplainer opex;
            if (paramNumber < 0) {
                opex = new CompoundExplainer(Type.LITERAL);
                opex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance(String.valueOf(invocation.getConstantValue(i))));
            }
            else {
                opex = new CompoundExplainer(Type.VARIABLE);
                opex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(paramNumber));
            }
            atts.put(Label.OPERAND, opex);
        }
        return atts;
    }

}
