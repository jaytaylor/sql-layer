
package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.types3.TParsers;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.common.TFormatter;
import com.akiban.server.types3.aksql.AkBundle;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.sql.types.TypeId;

/**
 * 
 * Implement AkServer's bool type which is a Java's primitive boolean
 */
public class AkBool
{
    public static final NoAttrTClass INSTANCE 
            = new NoAttrTClass(AkBundle.INSTANCE.id(), "boolean", AkCategory.LOGIC, TFormatter.FORMAT.BOOL, 1, 1, 1,
                               PUnderlying.BOOL, TParsers.BOOLEAN, 5, TypeId.BOOLEAN_ID);
}
