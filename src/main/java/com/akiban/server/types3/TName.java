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

package com.akiban.server.types3;

import com.akiban.util.ArgumentValidation;

import java.util.regex.Pattern;

public final class TName {

    public static String normalizeName(String name) {
        StringBuilder sb = new StringBuilder(name.length());
        String[] words = name.split("\\s+");
        for (int i = 0, wordsLength = words.length; i < wordsLength; i++) {
            String word = words[i];
            if (!WORD_VALIDATION.matcher(word).matches())
                throw new IllegalNameException("illegal type name: " + name);
            sb.append(word.toUpperCase());
            if (i+1 < wordsLength)
                sb.append(' ');
        }
        return sb.toString();
    }

    public String unqualifiedName() {
        return name;
    }

    public TBundleID bundleId() {
        return bundleID;
    }
    
    public String category() {
        return (category == null) ? "OTHER" : category.name();
    }

    // object interface

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TName other = (TName) o;
        return bundleID.equals(other.bundleID) && name.equals(other.name);
    }

    @Override
    public int hashCode() {
        int result = bundleID.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return bundleID + "_ " + name;
    }

    public TName(TBundleID bundleID, String name, Enum<?> category) {
        ArgumentValidation.notNull("bundle", bundleID);
        ArgumentValidation.notNull("name", name);
        this.bundleID = bundleID;
        this.name = normalizeName(name);
        this.category = category;
    }

    private final TBundleID bundleID;
    private final String name;
    private final Enum<?> category;
    private static final Pattern WORD_VALIDATION = Pattern.compile("[a-zA-Z][a-zA-Z0-9]*");
}
