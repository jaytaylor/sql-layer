
package com.akiban.server.types3.common.types;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.Serialization;
import com.akiban.server.types3.texpressions.SerializeAs;
import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.sql.types.CharacterTypeAttributes.CollationDerivation;

public enum StringAttribute implements Attribute
{
    /**
     * Number of characters
     * (Not byte length)
     */
    @SerializeAs(Serialization.LONG_1)MAX_LENGTH,

    @SerializeAs(Serialization.CHARSET) CHARSET,
    
    @SerializeAs(Serialization.COLLATION) COLLATION
    ;

    public static CharacterTypeAttributes characterTypeAttributes(TInstance tInstance) {
        Object cacheRaw = tInstance.getMetaData();
        if (cacheRaw != null) {
            return (CharacterTypeAttributes) cacheRaw;
        }
        CharacterTypeAttributes result;
        String charsetName = charsetName(tInstance);
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

    public static String charsetName(TInstance tInstance) {
        int charsetId = tInstance.attribute(CHARSET);
        return StringFactory.Charset.values()[charsetId].name();
    }

    public static TInstance copyWithCollation(TInstance tInstance, CharacterTypeAttributes cattrs) {
        AkCollator collator = AkCollatorFactory.getAkCollator(cattrs.getCollation());
        return tInstance.typeClass().instance(
                tInstance.attribute(MAX_LENGTH),
                tInstance.attribute(CHARSET), collator.getCollationId(),
                tInstance.nullability());
    }
}
