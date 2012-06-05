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

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.types3.TAttributeValue;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TypeDeclarationException;
import java.util.List;

public class StringFactory implements TFactory
{
    //--------------------------------CHARSET-----------------------------------
    //TODO: add more charsets as needed
    public static enum Charset
    {
        LATIN1, UTF8, UTF16
    }
    
    //--------------------------------COLLATION---------------------------------
    // TODO: not sure yet what we want to do about this
    
    //------------------------------Default values------------------------------
    
    // default number of characters in a string      
    private static final int DEFAULT_LENGTH = 255;
    
    private static final int DEFAULT_CHARSET_ID = Charset.UTF8.ordinal();
    
    private static final int DEFAULT_COLLATION_ID = 0; // TODO:
    
    //--------------------------------------------------------------------------
    
    private final TClass tclass;
    
    StringFactory(TClass tClass)
    {
        tclass = tClass;
    }
     
    /**
     * 
     * @param arguments: containing the attributes of a String. Should be in the following order:
     *          [<LENGTH>[,<CHARSET_ID>[,<COLLATION>]]]
     * @param strict ?? What's this flag for?
     * @return a type instance with the given attribute
     */
    @Override
    public TInstance create(List<TAttributeValue> arguments, boolean strict)
    {
        int length = DEFAULT_LENGTH;
        int charsetId = DEFAULT_CHARSET_ID;        
        int collation = DEFAULT_COLLATION_ID;
       
        switch (arguments.size())
        {
            case 3: // everything available
                collation = arguments.get(3).intValue(); // fall thru
            case 2: // avaialble up to charset
                charsetId = arguments.get(1).intValue(); // fall thru
            case 1: // avaialble up to length 
                length = arguments.get(0).intValue(); // fall thru
            case 0: // nothing available
                break;
            default:
                throw new TypeDeclarationException("too many arguments");
        }
        return new TInstance(tclass, length, charsetId, collation);
    }

}