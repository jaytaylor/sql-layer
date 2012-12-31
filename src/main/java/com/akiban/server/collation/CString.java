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
 * Holds a String and a collationId. Akiban Server registers a custom KeyCoder
 * instance to handle serializing and deserializing instances of this class in a
 * Persistit Key.
 * 
 * This class is not immutable because the CStringKeyCoder decoding methods are
 * designed to populate a hollow instance. However, the mutators should be used
 * only by the CStringKeyCoder and are therefore package-private.
 * 
 * @author peter
 * 
 */
public class CString {

    /**
     * String value
     */
    private String string;

    /**
     * Small integer handle that identifies the collation scheme.
     */
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
    void setString(String string) {
        this.string = string;
    }

    public int getCollationId() {
        return collationId;
    }

    /**
     * @param collationId
     *            the collationId to set
     */
    void setCollationId(int collationId) {
        this.collationId = collationId;
    }

    @Override
    public String toString() {
        return getString();
    }
}