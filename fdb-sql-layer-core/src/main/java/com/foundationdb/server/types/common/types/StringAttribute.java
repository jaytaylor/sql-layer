/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.types.common.types;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.Serialization;
import com.foundationdb.server.types.texpressions.SerializeAs;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.sql.types.CharacterTypeAttributes.CollationDerivation;

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

    public static CharacterTypeAttributes characterTypeAttributes(TInstance type) {
        Object cacheRaw = type.getMetaData();
        if (cacheRaw != null) {
            return (CharacterTypeAttributes) cacheRaw;
        }
        CharacterTypeAttributes result;
        String charsetName = charsetName(type);
        int collationId = type.attribute(COLLATION);
        if (collationId == StringFactory.NULL_COLLATION_ID) {
            result = new CharacterTypeAttributes(charsetName, null, null);
        }
        else {
            // TODO add implicit-vs-explicit
            String collationName = AkCollatorFactory.getAkCollator(collationId).getScheme();
            CollationDerivation derivation = CollationDerivation.IMPLICIT;
            result = new CharacterTypeAttributes(charsetName, collationName, derivation);
        }
        type.setMetaData(result);
        return result;
    }

    public static String charsetName(TInstance type) {
        int charsetId = type.attribute(CHARSET);
        return StringFactory.Charset.of(charsetId);
    }

    public static TInstance copyWithCollation(TInstance type, CharacterTypeAttributes cattrs) {
        AkCollator collator = AkCollatorFactory.getAkCollator(cattrs.getCollation());
        return type.typeClass().instance(
                type.attribute(MAX_LENGTH),
                type.attribute(CHARSET), collator.getCollationId(),
                type.nullability());
    }
}
