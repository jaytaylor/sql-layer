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

package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.types3.TParsers;
import com.akiban.server.types3.TParser;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.aksql.AkBundle;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.common.NumericFormatter;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.sql.types.TypeId;
public class AkNumeric {

    private AkNumeric() {}
    
    // numeric types
    public static final NoAttrTClass SMALLINT = create("smallint", NumericFormatter.FORMAT.INT_16, 1, 1, 2, PUnderlying.INT_16, TParsers.SMALLINT);
    public static final NoAttrTClass INT = create("int", NumericFormatter.FORMAT.INT_32, 1, 1, 4, PUnderlying.INT_32, TParsers.INT);
    public static final NoAttrTClass BIGINT = create("bigint", NumericFormatter.FORMAT.INT_64, 1, 1, 8, PUnderlying.INT_64, TParsers.BIGINT);
    public static final NoAttrTClass U_BIGINT = create("unsigned bigint", NumericFormatter.FORMAT.INT_64, 1, 1, 8, PUnderlying.INT_64, TParsers.UNSIGNED_BIGINT);
    
    public static final NoAttrTClass DOUBLE = create("double precision", NumericFormatter.FORMAT.DOUBLE, 1, 1, 8, PUnderlying.DOUBLE, TParsers.DOUBLE);

    // basically a curried function, with AkBunder.INSTANCE.id() partially applied
    private static NoAttrTClass create(String name,
                                       TClassFormatter formatter,
                                       int internalVersion,
                                       int serialVersion,
                                       int size,
                                       PUnderlying underlying,
                                       TParser parser)
    {
        return new NoAttrTClass(AkBundle.INSTANCE.id(), name, AkCategory.INTEGER, formatter, internalVersion, serialVersion, size,
                underlying, parser, TypeId.INTEGER_ID);
    }
}