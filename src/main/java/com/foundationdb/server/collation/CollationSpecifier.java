/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.collation;

import java.util.HashMap;

import com.foundationdb.server.error.AmbiguousCollationException;
import com.foundationdb.server.error.InvalidCollationSchemeException;

/**
 * This should take in a string representing a collation,
 * perform canonicalization, and provide an interface for
 * getting collation attributes 
 * 
 * (for use in AkCollatorFactory).
 * 
 * @author hchandl
 *
 */
public class CollationSpecifier {

    /*
     * Only the region needs to be checked specifically, for ambiguity
     */
    private final static int REGION_NDX = 1;

    private final static String CASE_SENSITIVE = "cs";
    private final static String CASE_INSENSITIVE = "ci";
    private final static String ACCENT_SENSITIVE = "co";
    private final static String ACCENT_INSENSITIVE = "cx";

    private final static String DEFAULT_CASE = CASE_SENSITIVE;
    private final static String DEFAULT_ACCENT = ACCENT_SENSITIVE;

    private String locale = null;
    private Boolean caseSensitive = null;
    private Boolean accentSensitive = null;
    private HashMap<String, String> keywordsToValues = new HashMap<String, String>();
    private String rawKeywordString = null;

    private final String originalScheme;

    public CollationSpecifier(String scheme) {
        originalScheme = scheme;
        canonicalizeCollation();
    }

    private void canonicalizeCollation() {
        String[] pieces = originalScheme.toLowerCase().split("_");
        if (pieces.length == 0) {
            throw new InvalidCollationSchemeException(originalScheme);
        }

        StringBuilder localeBuilder = new StringBuilder();
        Boolean localeStarted = false;
        Boolean localeFinished = false;
        Boolean ambiguousCase = false;
        Boolean ambiguousAccent = false;
        rawKeywordString = "";
        for (int i = 0; i < pieces.length; i++) {
            if (pieces[i].startsWith("@")) {
                rawKeywordString = pieces[i];
                processKeywords(pieces[i]);
                localeFinished = true;
            }
            else if (pieces[i].equals(CASE_SENSITIVE) || pieces[i].equals(CASE_INSENSITIVE)) {
                if (i == REGION_NDX) {
                    if (localeStarted) localeBuilder.append("_");
                    else localeStarted = true;
                    localeBuilder.append(pieces[i]);
                    ambiguousCase = true;
                } else {
                    setCaseAndAccent(pieces[i]);
                    localeFinished = true;
                }
            }
            else if (pieces[i].equals(ACCENT_SENSITIVE) || pieces[i].equals(ACCENT_INSENSITIVE)) {
                if (i == REGION_NDX) {
                    if (localeStarted) localeBuilder.append("_");
                    else localeStarted = true;
                    localeBuilder.append(pieces[i]);
                    ambiguousAccent = true;
                } else {
                    setCaseAndAccent(pieces[i]);
                    localeFinished = true;
                }
            }
            else if (localeFinished) {
                throw new InvalidCollationSchemeException(originalScheme);
            } else {
                if (localeStarted) localeBuilder.append("_");
                else localeStarted = true;
                localeBuilder.append(pieces[i]);
            }
        }
        
        // if the locale is just a language, need to append an underscore
        // makes things easier later (esp. with ambiguity)
        if (localeBuilder.indexOf("_") == -1) {
            localeBuilder.append("_");
        }
        locale = localeBuilder.toString();

        if ((ambiguousCase && caseSensitive == null) || (ambiguousAccent && accentSensitive == null)) {
            throwAmbiguousException(locale, rawKeywordString, pieces[REGION_NDX], ambiguousCase,
                                    ambiguousAccent, caseSensitive == null ? DEFAULT_CASE : caseSensitive ? CASE_SENSITIVE : CASE_INSENSITIVE, 
                                            accentSensitive == null ? DEFAULT_ACCENT : accentSensitive ? ACCENT_SENSITIVE: ACCENT_INSENSITIVE, originalScheme);
        }

        if (caseSensitive == null) caseSensitive = true;
        if (accentSensitive == null) accentSensitive = true;
    }

    private void setCaseAndAccent(String caseOrAccent) {
        // can't specify accent or case twice
        if (accentSensitive != null &&
                (caseOrAccent.equals(ACCENT_SENSITIVE) ||
                 caseOrAccent.equals(ACCENT_INSENSITIVE))) {
            throw new InvalidCollationSchemeException(originalScheme);
        }
        if (caseSensitive != null &&
                (caseOrAccent.equals(CASE_SENSITIVE) ||
                 caseOrAccent.equals(CASE_INSENSITIVE))) {
            throw new InvalidCollationSchemeException(originalScheme);
        }
        if (caseOrAccent.equals(CASE_SENSITIVE)) {
            caseSensitive = true;
        }
        else if (caseOrAccent.equals(CASE_INSENSITIVE)) {
            caseSensitive = false;
        }
        else if (caseOrAccent.equals(ACCENT_SENSITIVE)) {
            accentSensitive = true;
        }
        else if (caseOrAccent.equals(ACCENT_INSENSITIVE)) {
            accentSensitive = false;
        }
        else {
            // shouldn't actually hit here due to if statements above
            throw new InvalidCollationSchemeException(originalScheme);
        }
    }

    private void processKeywords(String keywords) {
        // skip the initial @ when parsing for keywords and values
        String[] keywordsAndValues = keywords.substring(1).split(";");
        
        for (String keywordAndValue : keywordsAndValues) {
            String[] pieces = keywordAndValue.split("=");
            if (pieces.length != 2) {
                throw new InvalidCollationSchemeException(originalScheme);
            }
            keywordsToValues.put(pieces[0], pieces[1]);
        }
    }

    private static void throwAmbiguousException(String locale, String rawKeywordString, String region, 
            Boolean ambiguousCase, Boolean ambiguousAccent, String providedCase, String providedAccent, 
            String scheme) {
        String possibility1 = new StringBuilder().append(locale.replace(region, ""))
                                                 .append("_")
                                                 .append(ambiguousCase ? region : providedCase == null ? DEFAULT_CASE : providedCase)
                                                 .append("_")
                                                 .append(ambiguousAccent ? region : providedAccent == null ? DEFAULT_ACCENT : providedAccent)
                                                 .toString();
        String possibility2 = new StringBuilder().append(locale)
                                                 .append("_")
                                                 .append(providedCase == null ? DEFAULT_CASE : providedCase)
                                                 .append("_")
                                                 .append(providedAccent == null ? DEFAULT_ACCENT : providedAccent)
                                                 .toString();
        throw new AmbiguousCollationException(scheme, possibility1, possibility2);
    }

    public boolean caseSensitive() {
        return caseSensitive;
    }

    public boolean accentSensitive() {
        return accentSensitive;
    }

    public String getLocale() {
        return locale;
    }

    public String getOriginalScheme() {
        return originalScheme;
    }

    public HashMap<String, String> getKeywordsAndValues() {
        return keywordsToValues;
    }

    public Boolean setStrength() {
        return keywordsToValues.isEmpty();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder().append(locale);
        if (!rawKeywordString.isEmpty()) {
            builder.append("_").append(rawKeywordString);
        } 
        else {
            builder.append("_")
                   .append(caseSensitive ? CASE_SENSITIVE : CASE_INSENSITIVE)
                   .append("_")
                   .append(accentSensitive ? ACCENT_SENSITIVE : ACCENT_INSENSITIVE);
        }
        return builder.toString();
    }
}
