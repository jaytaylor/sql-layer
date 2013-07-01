/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
        return StringFactory.Charset.of(charsetId);
    }

    public static TInstance copyWithCollation(TInstance tInstance, CharacterTypeAttributes cattrs) {
        AkCollator collator = AkCollatorFactory.getAkCollator(cattrs.getCollation());
        return tInstance.typeClass().instance(
                tInstance.attribute(MAX_LENGTH),
                tInstance.attribute(CHARSET), collator.getCollationId(),
                tInstance.nullability());
    }
}
