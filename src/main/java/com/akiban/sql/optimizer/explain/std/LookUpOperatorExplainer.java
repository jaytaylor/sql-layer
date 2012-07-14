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
package com.akiban.sql.optimizer.explain.std;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.*;

public class LookUpOperatorExplainer extends OperationExplainer
{
    public LookUpOperatorExplainer (String name,GroupTable gTable, RowType iRowType, boolean keepInput, Operator inputOp)
    {
        super(Type.LOOKUP_OPERATOR, buildAtts(name, gTable, iRowType, keepInput, inputOp));
    }
    
    private static Attributes buildAtts (String name,GroupTable gTable, RowType iRowType, boolean keepInput, Operator inputOp)
    {
        Attributes atts = new Attributes();
        
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(name));
        
        // TODO: is anything else needed in Group Table other than  its name?
        atts.put(Label.GROUP_TABLE, PrimitiveExplainer.getInstance(gTable.getName().getTableName().toString())); 
        
        atts.put(Label.INPUT_TYPE, PrimitiveExplainer.getInstance(iRowType.toString()));
        //if (keepInput != (Boolean)null)
        {
            atts.put(Label.LOOK_UP_OPTION, PrimitiveExplainer.getInstance((keepInput ? "" : "DO NOT") + "KEEP INPUT"));
        }
        //if (inputOp != (Operator)null)
        {
            atts.put(Label.INPUT_OPERATOR, inputOp.getExplainer());
        }
        
        return atts;
    }
}
