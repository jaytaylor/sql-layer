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

package com.akiban.server.types3;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class BooleanParserTest {

    @TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        param(builder, "1", true);
        param(builder, "1-1", true);
        param(builder, "1.1", true);
        param(builder, "1.1.1.1.1", true);
        param(builder, ".1", true); // this is weird. ".1" as tinyint is 0, and booleans in mysql are tinyint. but
                                    // (false OR ".1") results in a tinyint 1 (ie, true). Gotta love MySQL.
        param(builder, "0.1", true);
        param(builder, "-1", true);
        param(builder, "-1.1-a", true);
        param(builder, ".-1", false);
        param(builder, "-.1", true);
        param(builder, "-..1", false);
        param(builder, "1a", true);
        param(builder, "a1", false); // MySQL doesn't believe in steak sauce
        param(builder, "0", false);
        param(builder, "0.0", false);
        param(builder, "true", false);

        return builder.asList();
    }

    private static void param(ParameterizationBuilder builder, String string, boolean expected) {
        builder.add(string, string, expected);
    }

    public BooleanParserTest(String string, boolean expected) {
        this.string = string;
        this.expected = expected;
    }

    private String string;
    private boolean expected;

    @Test
    public void checkParse() {
        PValueSource source = new PValue(MString.varcharFor(string), string);
        PValue target = new PValue(AkBool.INSTANCE.instance(true));
        TParsers.BOOLEAN.parse(null, source, target);
        Boolean actual = target.isNull() ? null : target.getBoolean();
        assertEquals(string, Boolean.valueOf(expected), actual);
    }
}
