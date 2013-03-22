
package com.akiban.sql.server;

import com.akiban.server.explain.*;

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
