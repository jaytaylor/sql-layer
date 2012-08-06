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

import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.TAttributeValues;
import com.akiban.server.types3.TAttributesDeclaration;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.util.Enums;

import java.util.HashMap;
import java.util.Map;

public class StringFactory implements TFactory
{
    //--------------------------------CHARSET-----------------------------------
    //TODO: add more charsets as needed
    public static enum Charset
    {
        LATIN1, UTF8, UTF16, ISO_8859_1
        ;
        
        public static Charset of(String value) {
            // Could optimize this with a StringBuilder, for-loop, etc
            value = value.toUpperCase();
            Charset charset = lookupMap.get(value);
            if (charset == null)
                throw new AkibanInternalException("not a valid encoding: " + value);
            return charset;
        }
        
        public static String of (int ordinal)
        {
            return Charset.values()[ordinal].name();
        }

        private static final Map<String,Charset> lookupMap = createLookupMap();

        private static Map<String, Charset> createLookupMap() {
            Map<String,Charset> map = new HashMap<String, Charset>();
            for (Charset charset : Charset.values()) {
                map.put(charset.name(), charset);
            }
            // aliases
            map.put("ISO-8859-1", LATIN1);
            map.put("UTF-8", UTF8);
            map.put("UTF-16", UTF16);
            return map;
        }
    }
    
    //------------------------------Default values------------------------------
    
    // default number of characters in a string      
    protected static final int DEFAULT_LENGTH = 255;
    
    protected static final Charset DEFAULT_CHARSET = Charset.UTF8;
    
    protected static final int DEFAULT_COLLATION_ID = 0; // TODO:
    
    //--------------------------------------------------------------------------
    
    private final TClass tclass;
    
    public StringFactory(TClass tClass)
    {
        tclass = tClass;
    }
     
    /**
     * 
     *
     * @param declaration@return a type instance with the given attribute
     */
    @Override
    public TInstance create(TAttributesDeclaration declaration)
    {
        TAttributeValues values = declaration.validate(3, 3);
        int length = values.intAt(StringAttribute.LENGTH, DEFAULT_LENGTH);
        String charsetName = values.stringAt(StringAttribute.CHARSET, DEFAULT_CHARSET.name());
        int charsetId = Enums.ordinalOf(Charset.class, charsetName);
        String collationName = values.stringAt(StringAttribute.COLLATION, AkCollatorFactory.UCS_BINARY);
        int collation = AkCollatorFactory.getAkCollator(collationName).getCollationId();
        return tclass.instance(length, charsetId, collation);
    }

}