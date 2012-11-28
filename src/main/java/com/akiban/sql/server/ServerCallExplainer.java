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

package com.akiban.sql.server;

import com.akiban.server.explain.*;

public class ServerCallExplainer extends CompoundExplainer
{
    public ServerCallExplainer(ServerCallInvocation invocation, Attributes atts, ExplainContext context) {
        super(Type.PROCEDURE, addAttributes(atts, invocation, context));
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
