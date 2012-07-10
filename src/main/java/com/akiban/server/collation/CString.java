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

package com.akiban.server.collation;

/**
 * TODO: Prototype
 * 
 * Represent a String and an ICU4J Collator. Instances can be presented to the
 * {@link com.persistit.Key#append(Object)} method for encoding because Akiban
 * Server registers a {@link com.persistit.encoding.KeyRenderer} for this class.
 * The actual conversion from String to stored bytes is performed by the
 * Collator.
 * 
 * 
 * @author peter
 * 
 */
public class CString {

    private String string;

    private int collationId;

    /**
     * Construct an instance containing the original source string. This
     * instance may be used for encoding.
     * 
     * @param string
     * @param collator
     */
    public CString(final String string, final int collationId) {
        this.string = string;
        this.collationId = collationId;
    }
    
    public CString() {
        
    }

    public String getString() {
        return string;
    }

    /**
     * @param string
     *            the string to set
     */
    public void setString(String string) {
        this.string = string;
    }

    public int getCollationId() {
        return collationId;
    }

    /**
     * @param collationId
     *            the collationId to set
     */
    public void setCollationId(int collationId) {
        this.collationId = collationId;
    }
    
    @Override
    public String toString() {
        return getString();
    }
}