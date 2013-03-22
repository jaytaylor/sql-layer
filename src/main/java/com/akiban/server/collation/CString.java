
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