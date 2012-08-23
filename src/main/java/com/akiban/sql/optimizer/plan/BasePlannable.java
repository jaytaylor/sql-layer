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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.exec.Plannable;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.format.DefaultFormatter;
import com.akiban.server.explain.format.JsonFormatter;

import java.util.*;

/** Physical operator plan */
public abstract class BasePlannable extends BasePlanNode
{
    private Plannable plannable;
    private DataTypeDescriptor[] parameterTypes;
    
    protected BasePlannable(Plannable plannable,
                            DataTypeDescriptor[] parameterTypes) {
        this.plannable = plannable;
        this.parameterTypes = parameterTypes;
    }

    public Plannable getPlannable() {
        return plannable;
    }
    public DataTypeDescriptor[] getParameterTypes() {
        return parameterTypes;
    }

    public abstract boolean isUpdate();

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy operators.
    }
    
    public List<String> explainPlan(ExplainContext context, String defaultSchemaName) {
        DefaultFormatter f = new DefaultFormatter(defaultSchemaName, true);
        return f.format(plannable.getExplainer(context));
    }
    
    public String explainToJson(ExplainContext context) {
        JsonFormatter f = new JsonFormatter();
        return f.format(plannable.getExplainer(context));
    }

    public String explainToString(ExplainContext context, String defaultSchemaName) {
        return withIndentedExplain(new StringBuilder(getClass().getSimpleName()), context, defaultSchemaName);
    }

    @Override
    public String toString() {
        return explainToString(null, null);
    }

    @Override
    public String summaryString() {
        // Similar to above, but with @hash for consistency.
        return withIndentedExplain(new StringBuilder(super.summaryString()), null, null);
    }

    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName) {
        if (context == null)
            context = new ExplainContext(); // Empty
        for (String operator : explainPlan(context, defaultSchemaName)) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }

}
