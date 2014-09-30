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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.foundationdb.server.error.AmbiguousCollationException;
import com.foundationdb.server.error.InvalidCollationSchemeException;
import com.foundationdb.server.error.UnsupportedCollationException;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

/**
 * This should take in a string representing a collation,
 * perform canonicalization, and provide an interface for
 * getting collation attributes 
 * 
 * (for use in AkCollatorFactory).
 *
 */
public class CollationSpecifier {

    // Only the region needs to be checked, for ambiguity
    private final static int REGION_NDX = 1;

    private final static String CASE_SENSITIVE = "cs";
    private final static String CASE_INSENSITIVE = "ci";
    private final static String ACCENT_SENSITIVE = "co";
    private final static String ACCENT_INSENSITIVE = "cx";

    private final static String DEFAULT_CASE = CASE_SENSITIVE;
    private final static String DEFAULT_ACCENT = ACCENT_SENSITIVE;

    private String scheme;
    private String locale = null;
    private Boolean caseSensitive = null;
    private Boolean accentSensitive = null;
    private HashMap<String, String> keywordsToValues = new HashMap<String, String>();
    private String rawKeywordString = null; // convenient so this won't need to be rebuilt in toString()
    
    // Used to check the validity of requested locales
    private final static HashSet<ULocale> locales = new HashSet<ULocale>(Arrays.asList(ULocale.getAvailableLocales()));


    public CollationSpecifier(String scheme) {
        init(scheme);
    }

    private void init(String scheme) {
        this.scheme = scheme;
        String[] pieces = scheme.toLowerCase().split("_");

        StringBuilder localeBuilder = new StringBuilder();
        Boolean localeStarted = false;
        Boolean localeFinished = false;
        Boolean ambiguousCase = false;
        Boolean ambiguousAccent = false;
        rawKeywordString = "";
        for (int i = 0; i < pieces.length; i++) {
            if (pieces[i].startsWith("@")) {
                rawKeywordString = pieces[i];
                keywordsToValues = processKeywordString(pieces[i], scheme);
                localeFinished = true;
            }
            else if (isCaseString(pieces[i]) || isAccentString(pieces[i])) {
                if (i == REGION_NDX) {
                    if (localeStarted) localeBuilder.append("_");
                    localeStarted = true;

                    localeBuilder.append(pieces[i]);

                    if (isCaseString(pieces[i])) {
                        ambiguousCase = true;
                    } else {
                        ambiguousAccent = true;
                    }
                } else {
                    setCaseAndAccent(pieces[i]);
                    localeFinished = true;
                }
            }
            else if (localeFinished) {
                throw new InvalidCollationSchemeException(scheme);
            } else {
                if (localeStarted) localeBuilder.append("_");
                localeStarted = true;
                localeBuilder.append(pieces[i]);
            }
        }

        // if the locale is just a language, need to append an underscore
        // makes building the string easier/avoids ambiguity in REGION ndx
        if (localeBuilder.indexOf("_") == -1) {
            localeBuilder.append("_");
        }
        locale = localeBuilder.toString();

        checkAmbiguous(ambiguousCase, ambiguousAccent, pieces);

        if (caseSensitive == null) caseSensitive = CASE_SENSITIVE.equalsIgnoreCase(DEFAULT_CASE);
        if (accentSensitive == null) accentSensitive = ACCENT_SENSITIVE.equalsIgnoreCase(DEFAULT_ACCENT);
    }

    private void checkAmbiguous(Boolean ambiguousCase, Boolean ambiguousAccent, String[] pieces) {
        if ((ambiguousCase && caseSensitive == null) || 
                (ambiguousAccent && accentSensitive == null)) {
            String providedCase = caseSensitive == null ? DEFAULT_CASE : caseSensitive ? CASE_SENSITIVE : CASE_INSENSITIVE;
            String providedAccent = accentSensitive == null ? DEFAULT_ACCENT : accentSensitive ? ACCENT_SENSITIVE: ACCENT_INSENSITIVE;
            throwAmbiguousException(locale, rawKeywordString, pieces[REGION_NDX], ambiguousCase,
                                    ambiguousAccent, providedCase, providedAccent, scheme);
        }
    }

    public RuleBasedCollator createCollator() {
        ULocale ulocale = new ULocale(locale);
        checkLocale(ulocale, scheme);
        ulocale = setKeywords(ulocale, keywordsToValues);

        RuleBasedCollator collator = (RuleBasedCollator) RuleBasedCollator.getInstance(ulocale);
        checkKeywords(collator.getLocale(ULocale.VALID_LOCALE), keywordsToValues,
                scheme);

        if (shouldSetStrength()) {
            setCollatorStrength(collator, this);
        }
        
        return collator;
    }

    private static void checkKeywords(ULocale locale, Map<String, String> keywordsToValues, String scheme) {
        int count = 0;

        Iterator<String> localeKeywords = locale.getKeywords();
        while (localeKeywords != null && localeKeywords.hasNext()) {
            String keyword = localeKeywords.next();
            if (!keywordsToValues.containsKey(keyword) || 
                    !keywordsToValues.get(keyword).equalsIgnoreCase(locale.getKeywordValue(keyword))) {
                throw new UnsupportedCollationException(scheme);
            }
            count++;
        }

        if (count != keywordsToValues.size()) {
            throw new UnsupportedCollationException(scheme);
        }
    }

    private static void setCollatorStrength(RuleBasedCollator collator, CollationSpecifier specifier) {
        if (specifier.caseSensitive() && specifier.accentSensitive()) {
            collator.setStrength(Collator.TERTIARY);
            collator.setCaseLevel(false);
        }
        else if (specifier.caseSensitive() && !specifier.accentSensitive()) {
            collator.setCaseLevel(true);
            collator.setStrength(Collator.PRIMARY);
        }
        else if (!specifier.caseSensitive() && specifier.accentSensitive()) {
            collator.setStrength(Collator.SECONDARY);
            collator.setCaseLevel(false);
        }
        else {
            collator.setStrength(Collator.PRIMARY);
            collator.setCaseLevel(false);
        }
    }

    private static ULocale setKeywords(ULocale locale, Map<String, String> keywordsToValues) {
        for (Entry<String, String> entry : keywordsToValues.entrySet()) {
            locale = locale.setKeywordValue(entry.getKey(), entry.getValue());
        }
        return locale;
    }

    private static void checkLocale(ULocale locale, String scheme) {
        if (!locales.contains(locale))
            throw new UnsupportedCollationException(scheme);
    }

    private static Boolean isCaseString(String caseOrNot) {
        return caseOrNot.equalsIgnoreCase(CASE_INSENSITIVE) || 
               caseOrNot.equalsIgnoreCase(CASE_SENSITIVE);
    }

    private static Boolean isAccentString(String accentOrNot) {
        return accentOrNot.equalsIgnoreCase(ACCENT_INSENSITIVE) ||
               accentOrNot.equalsIgnoreCase(ACCENT_SENSITIVE);
    }

    private void setCaseAndAccent(String caseOrAccent) {
        // can't specify accent or case twice
        if (accentSensitive != null &&
                isAccentString(caseOrAccent)) {
            throw new InvalidCollationSchemeException(scheme);
        }
        if (caseSensitive != null &&
                isCaseString(caseOrAccent)) {
            throw new InvalidCollationSchemeException(scheme);
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
            throw new InvalidCollationSchemeException(scheme);
        }
    }

    private static HashMap<String, String> processKeywordString(String keywords, String scheme) {
        // skip the initial @ when parsing for keywords and values
        String[] keywordsAndValues = keywords.substring(1).split(";");

        HashMap<String, String> keywordsToValues = new HashMap<String, String>();
        for (String keywordAndValue : keywordsAndValues) {
            String[] pieces = keywordAndValue.split("=");
            if (pieces.length != 2) {
                throw new InvalidCollationSchemeException(scheme);
            }
            keywordsToValues.put(pieces[0], pieces[1]);
        }
        return keywordsToValues;
    }

    private static void throwAmbiguousException(String locale, String rawKeywordString, String region, 
            Boolean ambiguousCase, Boolean ambiguousAccent, String providedCase, String providedAccent, 
            String scheme) {
        String possibility1case = ambiguousCase ? region : providedCase == null ? DEFAULT_CASE : providedCase;
        String possibility1accent = ambiguousAccent ? region : providedAccent == null ? DEFAULT_ACCENT : providedAccent;
        String possibility1 = new StringBuilder().append(locale.replace(region, ""))
                                                 .append("_")
                                                 .append(possibility1case)
                                                 .append("_")
                                                 .append(possibility1accent)
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

    public HashMap<String, String> getKeywordsAndValues() {
        return keywordsToValues;
    }

    public Boolean shouldSetStrength() {
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
