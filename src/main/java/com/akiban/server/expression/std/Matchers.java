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
package com.akiban.server.expression.std;

import com.akiban.server.error.InvalidParameterValueException;
import java.util.*;

public final class Matchers
{
    private static final Matcher EMPTY = new Empty();
    
    public static Matcher getMatcher (String pattern, char escape, boolean ignoreCase)
    {
        if (pattern.isEmpty())
            return EMPTY;
        else if (pattern.charAt(0) == '%' && escape != '%')
            return new Contain(pattern, escape, ignoreCase);
        else 
            return new StartWith(pattern, escape, ignoreCase);
    }

    private static final class Token
    {
        final Map<Character, Integer> pos; // map each character to its right most position
        final char pattern[];              // the pattern string 
        final int length;                  // length of the patter (this is NOT neccessarily == pattern.length)
        final boolean ignoreCase;          
        final boolean endsWith;            // whether this Token is the last token
        
        Token (Map<Character, Integer> p, char pat[], int len, boolean ic, boolean end)
        {
            pos = p;
            pattern = pat;
            length = len;
            ignoreCase = ic;
            endsWith = end;
        }
    }
    
    /**
     * An empty pattern should only match empty string
     */
    static class Empty implements Matcher
    {
        @Override
        public int match(String str, int count)
        {
            return str.isEmpty() ? 0 : -1;
        }

        @Override
        public boolean sameState(String pattern, char escape)
        {
            return pattern.isEmpty();
        }
    }
    
    static abstract class AbstractMatcher implements Matcher
    {    
        // global variable, marking the position in the text string
        // should be altered in contain(...)
        protected int nextStart;
        
        /**
         * Search in <code>str</code> for a match with <code>tk</code>
         * If one is found, set nextStart to the index right after the match
         * 
         * @param str
         * @param start
         * @param limit
         * @param tk
         * @return <code>true</code> if <code>tk</code> is contained within the substring of
         * <code>str</code> starting at index <code>start</code> and ending at <code>limit<code>
         */
        protected boolean contain (String str, int start, int limit, Token tk)
        {
            char ch;
            int left = start, right = 0, lastPos = tk.length - 1;
            int tail;
            Integer d;
            
            While:
            while (left < limit)
            {
                if ((tail = left + lastPos) >= limit) // text is shorter than pattern
                    return false;
                
                // if mismatch does NOT occur at the end
                // then keep moving leftward from the tail
                if ((ch = str.charAt(tail)) == tk.pattern[right = lastPos]
                        || tk.pattern[right] == '\0' // wildcard _
                        || tk.ignoreCase && (ch = Character.toLowerCase(ch)) == tk.pattern[right])
                {
                    nextStart = tail + 1;
                    while (--tail >= left && --right >= 0)
                    {
                        if ((ch = str.charAt(tail)) != tk.pattern[right]
                                && (!tk.ignoreCase || (ch = Character.toLowerCase(ch)) != tk.pattern[right])
                                && tk.pattern[right] != '\0')
                        {
                            d = tk.pos.get(ch);
                            if (d == null) d = tk.pos.get('\0'); // if something doesn't match
                                                                 // try to find the underscore because that would match anything      

                            if (d != null && d < right) // if the mismatched is in pattern and if its position is
                                left += right - d;      // not greater than right (to prevent negative shifting)
                            else // if there's no such char in pattern
                                left += 1; // shift th pattern to the right by 1
                            continue While;
                        }
                    }
                    return true; // if something has gone wrong, we would've skipped it
                }
                else // if mismatch occurs at the end
                {
                    d = tk.pos.get(ch);
                    if (d == null) d = tk.pos.get('\0'); // if something isn't  in there, try to get the right most wildcard _
                                                         // because it'd match anything
                    left += d == null ? tk.length : right - d;
                }
            }
            return false; // if the two matched, we would've returned true
        }
    }
    
    public static class Index extends AbstractMatcher
    {
        private final Token tk;
        private final String pattern;
        
        public Index(String st)
        {
            Map<Character, Integer> map = new HashMap<Character, Integer>();
            int n;
            for ( n = 0; n < st.length(); ++n)
                map.put(st.charAt(n), n);
            tk = new Token(map, st.toCharArray(), n, false, false);
            pattern = st;
        }
        
        @Override
        public int match(String str, int count)
        {
            nextStart = 0;
            do
            {
                if (!contain(str, nextStart, str.length(), tk))
                    return -1; // didn't find anything
                --count;
            }
            while (count != 0);
            return nextStart - tk.length; // return the index at which the pattern starts
        }

        @Override
        public boolean sameState(String pattern, char escape)
        {
            return pattern.equals(this.pattern);
        }
    }

    /**
     * the pattern starts with percent
     */
    static class Contain extends AbstractMatcher
    {
        private final String pattern;
        private final char escape;
        private List<Token> tokens;
        
        public Contain (String pt, char escape, boolean ignoreCase)
        {
            pattern = pt;
            this.escape = escape;
            
            // start at 0, since 0 would've been skipped anyways
            tokens = computePos(escape, pt, 1, ignoreCase);
        }
        
        @Override
        public int match(String str, int count)
        {   
            nextStart  = 0;
            for (Token tk : tokens)
                if (tk.endsWith ? !endWith(str, nextStart, tk)
                                : !contain(str, nextStart, str.length(), tk))
                    return -1;
            
            return 1; // has no need for the index, thus just return 1 to indicate a 'match'
        }

        @Override
        public boolean sameState(String pattern, char escape)
        {
            return pattern.equals(this.pattern) && escape == this.escape;
        }
    }
    
    
    static class StartWith extends  AbstractMatcher
    {
        private final String pattern;
        private final char escape;
        private final boolean ignoreCase;
        
        private List<Token> tokens;
        
        public StartWith (String pt, char escape, boolean ic)
        {
            ignoreCase = ic;
            pattern = pt;
            this.escape = escape;
        }
        
        private void doComputePos(int start)
        {
            tokens = new LinkedList<Token>();
            
            tokens = computePos(escape, pattern, start, ignoreCase);
        }
        
        @Override
        public int match(String str, int count)
        {
            char lch, rch;
            int lLimit = str.length(), rLimit = pattern.length();
            int r_len = pattern.length();
            int l = 0, r = 0;
            
            boolean percent = false;
            
            while (r < r_len && l < lLimit)
            {
                if ((rch = pattern.charAt(r)) == escape)
                {
                    if (r + 1 >= rLimit)
                        throw new InvalidParameterValueException("Illegal Escaped Sequence");
                    if ((rch = pattern.charAt(r +1)) != (lch = str.charAt(l))
                            && (!ignoreCase || Character.toLowerCase(rch) != Character.toLowerCase(lch)))
                        return -1;
                    r += 2;
                    ++l;
                }
                else if (rch == '%')
                {
                    if (r + 1 == r_len) // end of string
                        return 1; // has no need for the index, so just return a non negative value
                    
                    percent = true;
                    break;
                }
                else if (rch == '_')
                {
                    ++r;
                    ++l;
                }
                else
                {
                    if (rch != (lch = str.charAt(l))
                            && (!ignoreCase || Character.toLowerCase(rch) != Character.toLowerCase(lch)))
                        return -1;
                    ++l;
                    ++r;
                }
            }
            
            if (percent)
            {
                nextStart = l;
                if (tokens == null)
                    doComputePos(r);
                
                for (Token tk : tokens)
                    if (tk.endsWith ? !endWith(str, nextStart, tk)
                                    : !contain(str, nextStart, str.length(), tk))
                        return -1;
                return 1;
            }
            else // at least one of the string reaches its end
            {
                if ( l == str.length())
                {
                    while (r < r_len)
                    {
                        // could throw an illegal escape sequence here
                        // but we could just simply return false
                        // it's less likely anyone would intentionally do something just to get an exception
                        if (pattern.charAt(r) != '%' || escape == '%') // not % or %  escaped by itself
                            return -1;
                        ++r;
                    }
                    return 1;
                }
                else 
                    return -1;
            }
        }

        @Override
        public boolean sameState(String pattern, char escape)
        {
            return pattern.equals(this.pattern) && escape == this.escape;
        }
    }
    
    //------------ static helpers-----------------------------------------------
          
    /**
     * Divide the pattern into multiple tokens (each separated by %)
     * 
     * + Iterates thru all characters in the pattern string:
     *           - if a regular char is encountered, put it in a char array
     *           - if an escape char is encountered, just put the character that is 'escaped' by it
     *           - if a wildcard _ is encountered, mark it as \0 (null char) because it's special!
     *           - if a wildcard % is encountered, stop the search, put the token into
     *             the list. Also determine wether this is the last token (to speed up the matching)
     *              
     * 
     * + Finds and maps each distinct character with its right most position
     * 
     * @param escape
     * @param pat
     * @param start     : start position in the string pattern
     * @param ignoreCase
     * @return          : return the list of all tokens
     */
    private static List<Token> computePos (char escape, String pat, int start, boolean ignoreCase)
    {
        final int patLength = pat.length();
        int n = start;
        int length; // length of the token
        
        char pt[]; // contains the token after it was 'processed'
        char ch;
        
        boolean hasLetter;
        
        // map each character to its right most position
        Map<Character, Integer> shift;
        
        // list of all tokens (each separated by '%'s
        List<Token> ret = new LinkedList<Token>();
        
        while (n < patLength)
        {
        
            pt = new char[patLength - start];
            length = 0;
            hasLetter = false;
            shift = new HashMap<Character, Integer>();
            
            For:
            for (; n < patLength; ++n)
            {
                if ((ch = pat.charAt(n)) == escape)
                {
                    if (++n < patLength)
                    {
                        ch = pat.charAt(n);
                        shift.put(pt[length] = ignoreCase && (hasLetter |= Character.isLetter(ch))
                                                        ? Character.toLowerCase(ch)
                                                        : ch
                              , length);

                        ++length;
                    }
                    else
                        throw new InvalidParameterValueException("Illegal escaped sequence");
                }
                else if (ch == '%')
                {
                    // skip multiples %'s
                    do
                        ++n;
                    while (n < patLength && pat.charAt(n) == '%');

                    if (length == 0 && (n < patLength || pat.charAt(n -1) != '%'))
                    {
                        --n;
                        continue For;
                    }
                    else 
                        break For;
                }
                else if (ch == '_')
                {
                    shift.put(pt[length] = '\0', length);
                    ++length;
                }
                else
                {
                   shift.put(pt[length] = ignoreCase && (hasLetter |= Character.isLetter(ch))    
                                                    ? Character.toLowerCase(ch)
                                                    : ch
                            , length);

                    ++length;
                }
            }

            if (length > 0) ret.add(new Token(shift, 
                    pt, 
                    length, 
                    ignoreCase && hasLetter,
                    n < patLength
                        ? false 
                        : !(pat.charAt(n-1) == '%' && ((n - 2 < 0) || pat.charAt(n-2) != escape))));
        }
        return ret;
    }

    /**
     * Check if a string ends with a given pattern
     * 
     * @param str
     * @param start
     * @param tk
     * @return 
     */
    private static boolean endWith(String str, int start, Token tk)
    {
        int left = str.length() -1;
        int right = tk.length -1;

        // goes from the end
        while (left >= start && right >= 0)
        {
            if (tk.pattern[right] == str.charAt(left)
                    || tk.pattern[right] == '\0'
                    || tk.ignoreCase && tk.pattern[right] == Character.toLowerCase(str.charAt(left))
               )
            {
                --right;
                --left;
            }
            else
                return false;
        }

        // if right exhausts, that's ok
        return right <0;
    }
}
