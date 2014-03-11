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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.error.InvalidParameterValueException;

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class Matchers
{
    public static Matcher getMatcher(String pattern, char escape, boolean ignoreCase) {
        if(pattern.isEmpty()) {
            return new EmptyMatcher();
        }
        TokenSet ts = buildTokenSet(pattern, escape, ignoreCase);
        if((ts.startsWith != null) && (ts.startsWith == ts.endsWith)) {
            assert ts.contains.length == 0;
            return new EqualsMatcher(pattern, escape, ignoreCase, ts.startsWith);
        }
        return new GenericMatcher(pattern, escape, ignoreCase, ts);
    }

    private static final class Token
    {
        final char[] chunk;
        final Map<Character, Integer> rightIndex;
        final BitSet wildIndex;

        private Token(char[] chunk, Map<Character, Integer> rightIndex, BitSet wildIndex) {
            this.rightIndex = rightIndex;
            this.chunk = chunk;
            this.wildIndex = wildIndex;
        }

        /** Get right most index that would match {@code c}. */
        public Integer getRightIndex(char c) {
            Integer r = rightIndex.get(c);
            // If no match, find the right-most wildcard as it matches anything.
            if(r == null) {
                r = wildIndex.length() - 1;
                if(r == -1) {
                    r = null;
                }
            }
            return r;
        }
    }

    private static final class TokenSet
    {
        private final Token startsWith;
        private final Token[] contains;
        private final Token endsWith;

        public TokenSet(Token startsWith, Token[] contains, Token endsWith) {
            this.startsWith = startsWith;
            this.contains = contains;
            this.endsWith = endsWith;
        }
    }

    private static void checkEndEscape(String pattern, char escape) {
        int len = pattern.length();
        if(pattern.charAt(len - 1) == escape) {
            if(((len - 2) < 0) || (pattern.charAt(len - 2) != escape)) {
                throw new InvalidParameterValueException("Illegal escape sequence");
            }
        }
    }

    private static boolean isExactStart(String pattern, char escape) {
        return (pattern.charAt(0) != '%') && (escape != '%');
    }

    private static boolean isExactEnd(String pattern, char escape) {
        int len = pattern.length();
        if(pattern.charAt(len - 1) != '%') {
            return true;
        }
        // Still exact if percent is escaped but not if the escape is escaped
        boolean hasN2 = (len - 2) >= 0;
        boolean hasN3 = (len - 3) >= 0;
        return hasN2 && (pattern.charAt(len - 2) == escape) && (hasN3 && pattern.charAt(len - 3) != escape);
    }

    /** Split the pattern into (unescaped) % delimited Tokens. */
    private static TokenSet buildTokenSet(String pattern, char escape, boolean doLowerCase) {
        assert !pattern.isEmpty();
        checkEndEscape(pattern, escape);
        final int patLength = pattern.length();
        List<Token> tokens = new LinkedList<>();
        for(int n = 0; n < patLength; /*none*/) {
            char[] chunk = new char[patLength - n];
            int chunkLen = 0;
            Map<Character, Integer> rightIndex = new HashMap<>();
            BitSet wildIndex = new BitSet();
            for(; n < patLength; ++n) {
                char ch = pattern.charAt(n);
                if(ch == escape) {
                    assert (n + 1) < patLength : pattern;
                    ch = pattern.charAt(++n);
                } else if(ch == '%') {
                    ++n;
                    // Split
                    break;
                } else if(ch == '_') {
                    wildIndex.set(chunkLen);
                }
                if(doLowerCase) {
                    ch = Character.toLowerCase(ch);
                }
                chunk[chunkLen] = ch;
                rightIndex.put(ch, chunkLen++);
            }
            if(chunkLen > 0) {
                if(chunk.length != chunkLen) {
                    chunk = Arrays.copyOf(chunk, chunkLen);
                }
                if(wildIndex.size() < chunkLen) {
                    wildIndex.set(chunkLen, false);
                }
                tokens.add(new Token(chunk, rightIndex, wildIndex));
            }
        }
        Token startsWith = null;
        if(!tokens.isEmpty() && isExactStart(pattern, escape)) {
            startsWith = tokens.remove(0);
        }
        Token endsWith = null;
        if(isExactEnd(pattern, escape)) {
            if(tokens.isEmpty()) {
                endsWith = startsWith;
            } else {
                endsWith = tokens.remove(tokens.size() - 1);
            }
        }
        return new TokenSet(startsWith, tokens.toArray(new Token[tokens.size()]), endsWith);
    }

    /** Find the first location of the token and return the following index, -1 if not found. */
    private static int findToken(Token token, String str, int startIndex, boolean doLowerCase) {
        final int tokMax = token.chunk.length - 1;
        final int strLength = str.length();
        int left = startIndex;
        outer:
        while(left < strLength) {
            int tail = left + tokMax;
            if(tail >= strLength) {
                // Text is shorter than pattern
                return -1;
            }
            // If mismatch does NOT occur at the end then keep moving leftward from the tail
            char ch = str.charAt(tail);
            if(doLowerCase) {
                ch = Character.toLowerCase(ch);
            }
            int right = tokMax;
            if((ch == token.chunk[right]) || token.wildIndex.get(right)) {
                int nextStart = tail + 1;
                while((--tail >= left) && (--right >= 0)) {
                    ch = str.charAt(tail);
                    if(doLowerCase) {
                        ch = Character.toLowerCase(ch);
                    }
                    if((ch != token.chunk[right]) && !token.wildIndex.get(right)) {
                        Integer d = token.getRightIndex(ch);
                        if((d != null) && (d < right)) {
                            // Mismatch is in pattern and rightmost is within how much of pattern has been used
                            left += right - d;
                        } else {
                            // Shift pattern right by 1 iff it has no such char
                            left += 1;
                        }
                        continue outer;
                    }
                }
                // Would have skipped a mismatch
                return nextStart;
            } else {
                // Mismatch occurs at the end;
                Integer d = token.getRightIndex(ch);
                left += (d == null) ? token.chunk.length : right - d;
            }
        }
        // Would have already returned true if there was a match
        return -1;
    }

    /** Check if {@code tokens} matches {@code str} at {@code startIndex}. */
    private static boolean tokensMatch(Token[] tokens, String str, int startIndex, boolean doLowerCase) {
        int nextStart = startIndex;
        boolean matched = true;
        for(int i = 0; matched && (i < tokens.length); ++i) {
            nextStart = findToken(tokens[i], str, nextStart, doLowerCase);
            matched = (nextStart >= 0);
        }
        return matched;
    }

    /** Check if a string ends with a given pattern */
    private static boolean isExactMatch(Token token, String str, int startIndex, boolean doLowerCase) {
        if((startIndex < 0) || (startIndex + token.chunk.length) > str.length()) {
            return false;
        }
        for(int ti = 0, si = startIndex; ti < token.chunk.length; ++ti, ++si) {
            char tch = token.chunk[ti];
            char sch = str.charAt(si);
            if(doLowerCase) {
                sch = Character.toLowerCase(sch);
            }
            if((tch != sch) && !token.wildIndex.get(ti)) {
                return false;
            }
        }
        return true;
    }

    private static class EmptyMatcher implements Matcher
    {
        @Override
        public boolean matches(String str) {
            return str.isEmpty();
        }

        @Override
        public boolean sameState(String pattern, char escape) {
            return pattern.isEmpty();
        }
    }

    private static abstract class AbstractMatcher implements Matcher
    {
        protected final String pattern;
        protected final char escape;
        protected final boolean ignoreCase;

        private AbstractMatcher(String pattern, char escape, boolean ignoreCase) {
            this.pattern = pattern;
            this.escape = escape;
            this.ignoreCase = ignoreCase;
        }

        @Override
        public boolean sameState(String pattern, char escape) {
            return this.pattern.equals(pattern) && (this.escape == escape);
        }
    }

    private static class EqualsMatcher extends AbstractMatcher
    {
        private final Token token;

        private EqualsMatcher(String pattern, char escape, boolean ignoreCase, Token token) {
            super(pattern, escape, ignoreCase);
            this.token = token;
        }

        @Override
        public boolean matches(String str) {
            return (str.length() == token.chunk.length) &&
                isExactMatch(token, str, 0, ignoreCase);
        }
    }

    private static class GenericMatcher extends AbstractMatcher
    {
        private final TokenSet tokenSet;

        public GenericMatcher(String pattern, char escape, boolean ignoreCase, TokenSet tokenSet) {
            super(pattern, escape, ignoreCase);
            this.tokenSet = tokenSet;
        }

        @Override
        public boolean matches(String str) {
            int startIndex = (tokenSet.startsWith != null) ? tokenSet.startsWith.chunk.length : 0;
            return matchesStartsWith(str) &&
                tokensMatch(tokenSet.contains, str, startIndex, ignoreCase) &&
                matchesEndsWith(str);
        }

        private boolean matchesStartsWith(String str) {
            return (tokenSet.startsWith == null) ||
                isExactMatch(tokenSet.startsWith, str, 0, ignoreCase);
        }

        private boolean matchesEndsWith(String str) {
            return (tokenSet.endsWith == null) ||
                isExactMatch(tokenSet.endsWith, str, str.length() - tokenSet.endsWith.chunk.length, ignoreCase);
        }
    }

    /** Matches against an pattern a number of times. Pattern does not use escape, _ or %. */
    public static class IndexMatcher implements Matcher
    {
        private final String pattern;
        private final Token token;

        public IndexMatcher(String pattern) {
            this.pattern = pattern;
            Map<Character, Integer> rightPos = new HashMap<>();
            for(int i = 0; i < pattern.length(); ++i) {
                rightPos.put(pattern.charAt(i), i);
            }
            this.token = new Token(pattern.toCharArray(), rightPos, new BitSet(pattern.length()));
        }

        @Override
        public boolean matches(String str) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean sameState(String pattern, char escape) {
            return this.pattern.equals(pattern);
        }

        /** Returns the index at which {@code str} matched {@code count} times. */
        public int matchesAt(String str, int count) {
            int nextStart = 0;
            for(int i = 0; i < count; ++i) {
                nextStart = findToken(token, str, nextStart, false);
                if(nextStart < 0) {
                    return -1;
                }
            }
            // Index where count pattern starts
            return nextStart - token.chunk.length;
        }
    }
}
