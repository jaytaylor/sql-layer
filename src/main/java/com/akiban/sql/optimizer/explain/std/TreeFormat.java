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

import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Format;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import java.util.Map.Entry;

/**
 * 
 * DEFUNCT DO NOT USE. 
 * 
 */
public class TreeFormat // used to implement Format.java, now is outdated.
{

    public String describe(Explainer explainer)
    {
        StringBuilder bd = new StringBuilder();
        doFormat(explainer, bd.append("\n"), 0);
        
        return bd.append("\n").toString();
    }
    
    private static void doFormat (Explainer ex, StringBuilder bd, int level)
    {
        if (ex.hasAttributes())
        {
             OperationExplainer opEx = (OperationExplainer)ex;
             Attributes atts = (Attributes) opEx.get().clone();
             
             // print name
             bd.append(atts.get(Label.NAME).get(0).get()); 
             atts.remove(Label.NAME);
             ++level;
             for (Entry<Label, Explainer> entry : atts.valuePairs())
             {
                 bd.append("\n");
                 for (int i = 0; i < level; ++i)
                     bd.append("--");
                 bd.append(entry.getKey()).append(": ");
                 doFormat(entry.getValue(), bd, level +1);
             }
        }
        else
            bd.append(((PrimitiveExplainer)ex).get());
    }
    
}
