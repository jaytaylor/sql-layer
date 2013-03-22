
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
    
    public String categoryName() {
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
