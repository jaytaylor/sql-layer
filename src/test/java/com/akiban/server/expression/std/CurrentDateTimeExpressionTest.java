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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.AkType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.Expression;
import org.junit.Test;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import static org.junit.Assert.assertEquals;
import java.util.Date;

public class CurrentDateTimeExpressionTest
{

    private final Date RIGHT_NOW = new Date();
    private final Date EPOCH = new Date(0);
    private int position = 0;

    @Test
    public void testCurrentDate ()
    {
        test(RIGHT_NOW, AkType.DATE, "yyyy-MM-dd");
        test(EPOCH, AkType.DATE,"yyyy-MM-dd");
    }

    @Test
    public void testCurrentTime ()
    {
        test(RIGHT_NOW, AkType.TIME, "HH:mm:ss");
        test(EPOCH, AkType.TIME, "HH:mm:ss");
    }

    @Test
    public void testCurrentTimestamp ()
    {
        test(RIGHT_NOW, AkType.DATETIME, "yyyy-MM-dd HH:mm:ss");
        test(EPOCH, AkType.DATETIME, "yyyy-MM-dd HH:mm:ss");
    }

    private void test (final Date when, AkType current_, String strFrm)
    {
        Expression ex = new CurrentDateTimeExpression(current_);
        ExpressionEvaluation eval = ex.evaluation();
        eval.of(new SimpleQueryContext(null) {
                @Override
                public Date getCurrentDate() {
                    return when;
                }
            });

        assertEquals(Extractors.getLongExtractor(current_).getLong(eval.eval()),
                     Extractors.getLongExtractor(current_).getLong(new DateTime(when).toString(DateTimeFormat.forPattern(strFrm))));

    }
}
