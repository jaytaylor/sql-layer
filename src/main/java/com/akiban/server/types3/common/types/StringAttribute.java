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

package com.akiban.server.types3.common.types;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.sql.types.CharacterTypeAttributes.CollationDerivation;

public enum StringAttribute implements Attribute
{
    /**
     * Number of characters
     * (Not byte length)
     */
    LENGTH,

    CHARSET,
    
    COLLATION
    ;

    public static CharacterTypeAttributes characterTypeAttributes(TInstance tInstance) {
        Object cacheRaw = tInstance.getMetaData();
        if (cacheRaw != null) {
            return (CharacterTypeAttributes) cacheRaw;
        }
        CharacterTypeAttributes result;
        int charsetId = tInstance.attribute(CHARSET);
        String charsetName = StringFactory.Charset.values()[charsetId].name();
        int collationId = tInstance.attribute(COLLATION);
        if (collationId == StringFactory.NULL_COLLATION_ID) {
            result = new CharacterTypeAttributes(charsetName, null, null);
        }
        else {
            // TODO add implicit-vs-explicit
            String collationName = AkCollatorFactory.getAkCollator(collationId).getName();
            CollationDerivation derivation = CollationDerivation.IMPLICIT;
            result = new CharacterTypeAttributes(charsetName, collationName, derivation);
        }
        tInstance.setMetaData(result);
        return result;
    }

    public static TInstance copyWithCollation(TInstance tInstance, CharacterTypeAttributes cattrs) {
        AkCollator collator = AkCollatorFactory.getAkCollator(cattrs.getCollation());
        return tInstance.typeClass().instance(tInstance.attribute(LENGTH), tInstance.attribute(CHARSET), collator.getCollationId());
    }
}
