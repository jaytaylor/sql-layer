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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        final Map<Character, Integer> pos;
        final char pattern[];
        final int length;
        final boolean ignoreCase;
        final boolean endsWith;
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
     * An emptry pattern should only match empty string
     */
    static class Empty implements Matcher
    {
        @Override
        public boolean match(String str)
        {
            return str.isEmpty();
        }
    }
    
    /**
     * the pattern starts with percent
     */
    static class Contain implements Matcher
    {
        private final String pattern;
        private List<Token> tokens;
        
        public Contain (String pt, char escape, boolean ignoreCase)
        {
            pattern = pt;
            tokens = new LinkedList<Token>();
            
            // start at 1. (0 is %, which would've been skipped anyways)
            int start = 1;
            do
                start = computePos(escape, pt, start, tokens, ignoreCase);
            while (start < pattern.length());
        }

        // global variable, marking the position in the text string
        // should be altered in contain(...)
        private int nextStart;
        private boolean contain (String str, int start, int limit, Token tk)
        {
            int left = start, right = 0, lastPos = tk.length - 1;
            int tail;
            Integer d;
            
            While:
            while (left < limit)
            {
                if ((tail = left + lastPos) >= limit)
                    return false; // ??? pattern is longer than text???
                
                if (str.charAt(tail) == tk.pattern[right = lastPos]
                        || tk.ignoreCase && Character.toLowerCase(str.charAt(tail)) == tk.pattern[right]
                        || tk.pattern[right] == '\0') // wildcard _
                {
                    nextStart = tail + 1; // set the next position in left
                    while (--tail >= left && --right >= 0) // walks backward from the tail 
                    {                                      // to the head looking for mismatched chars
                        if (str.charAt(tail) != tk.pattern[right]
                                && (!tk.ignoreCase || Character.toLowerCase(str.charAt(tail)) != tk.pattern[right])
                                && tk.pattern[right] != '\0')
                        {
                            d = tk.pos.get(tk.ignoreCase? Character.toLowerCase(str.charAt(tail)) : str.charAt(tail));
                            if (d == null) d = tk.pos.get('\0'); // if something doesn't match
                                                                 // try to find the underscore because that would match anything
                            left += d == null || (d - 1) <= lastPos // can't shift to the left
                                    ? tk.length 
                                    : tk.length - d;
                            continue While;
                        }
                    }
                    return true; // if someting has gone wrong, we would've skipped it
                }
                else
                {
                    d = tk.pos.get(tk.ignoreCase? Character.toLowerCase(str.charAt(tail)) : str.charAt(tail));
                    if (d == null) d = tk.pos.get('\0'); // if something isn't  in there, try to get the right most wildcard _
                                                         // because it'd match anything      
                    left += d == null ? tk.length: tk.length - d;
                }
            }
            return false; // if the two matched, we would've returned true
        }
        
        @Override
        public boolean match(String str)
        {   
            nextStart  = 0;
            for (Token tk : tokens)
                if (tk.endsWith ? !endWith(str, nextStart, tk)
                                : !contain(str, nextStart, str.length(), tk))
                    return false;
            
            return true;
        }
    }
    
    
    static class StartWith implements Matcher
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
            
            do
                start = computePos(escape, pattern, start, tokens, ignoreCase);
            while (start < pattern.length());
        }
        
        
        // global variable, marking the position in the text string
        // should be altered in contain(...)
        private int nextStart;
        private boolean contain (String str, int start, int limit, Token tk)
        {
            int left = start, right = 0, lastPos = tk.length - 1;
            int tail;
            Integer d;
            
            While:
            while (left < limit)
            {
                if ((tail = left + lastPos) >= limit)
                    return false; // ??? pattern is longer than text???
                
                if (str.charAt(tail) == tk.pattern[right = lastPos]
                        || tk.ignoreCase && Character.toLowerCase(str.charAt(tail)) == tk.pattern[right]
                        || tk.pattern[right] == '\0') // wildcard _
                {
                    nextStart = tail + 1;
                    while (--tail >= left && --right >= 0)
                    {
                        if (str.charAt(tail) != tk.pattern[right]
                                && (!tk.ignoreCase || Character.toLowerCase(str.charAt(tail)) != tk.pattern[right])
                                && tk.pattern[right] != '\0')
                        {
                            d = tk.pos.get(tk.ignoreCase? Character.toLowerCase(str.charAt(tail)) : str.charAt(tail));
                            if (d == null) d = tk.pos.get('\0'); // if something doesn't match
                                                                 // try to find the underscore because that would match anything      
                            left += d == null || (d - 1) <= lastPos // can't shift to the left
                                    ? tk.length
                                    : tk.length - d;
                            continue While;
                        }
                    }
                    return true; // if someting has gone wrong, we would've skipped it
                }
                else
                {
                    d = tk.pos.get(tk.ignoreCase? Character.toLowerCase(str.charAt(tail)) : str.charAt(tail));
                    if (d == null) d = tk.pos.get('\0'); // if something isn't  in there, try to get the right most wildcard _
                                                         // because it'd match anything
                         
                    left += d == null //|| d -1 <= tail
                            ? tk.length
                            : tk.length - d;
                }
            }
            return false; // if the two matched, we would've returned true
        }
        
        @Override
        public boolean match(String str)
        {
            int limit = str.length();
            int r_len = pattern.length();

            boolean percent = false;
            int l = 0, r = 0;
            
            while (r < r_len && l < str.length())
            {
                if (pattern.charAt(r) == escape)
                {
                    if (r + 1 >= pattern.length())
                        throw new InvalidParameterValueException("Illegal Escaped Sequence");
                    if (pattern.charAt(r +1) != str.charAt(l)
                            && (!ignoreCase || Character.toLowerCase(pattern.charAt(r+1)) 
                                                != Character.toLowerCase(str.charAt(l))))
                        return false;
                    r += 2;
                    ++l;
                }
                else if (pattern.charAt(r) == '%')
                {
                    if (r + 1 == r_len) // end of string
                        return true;
                    
                    percent = true;
                    break;
                }
                else if (pattern.charAt(r) == '_')
                {
                    ++r;
                    ++l;
                }
                else
                {
                    if (pattern.charAt(r) != str.charAt(l)
                            && (!ignoreCase || Character.toLowerCase(pattern.charAt(r))
                                               != Character.toLowerCase(str.charAt(l))))
                        return false;
                    ++l;
                    ++r;
                }
            }
            
            
            if (percent)
            {
                nextStart = l;
                
                // check escaped
                if (tokens == null)
                    doComputePos(r);
                
                for (Token tk : tokens)
                    if (tk.endsWith ? !endWith(str, nextStart, tk)
                                    : !contain(str, nextStart, str.length(), tk))
                        return false;
                return true;
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
                            return false;
                        ++r;
                    }
                    return true;
                }
                else 
                    return false;
            }
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
     *           - if a wildcard % is encountered, stop the search return the position where the search
     *             stops
     * 
     * + Finds and maps each distinct character with its right most position
     * 
     * + If this is the last token in the pattern and there's nothing behind it
     *      then set the endsWith flag, so that the match can just skip to the end
     *      of the string. 
     * 
     * + Put all the info gathered about this token in the list 'ret'
     * @param escape
     * @param pat
     * @param start     : start position in the string pattern
     * @param ret
     * @param ignoreCase
     * @return          : return the next position to search in the pattern
     */
    private static int computePos (char escape, String pat, int start, List<Token> ret, boolean ignoreCase)
    {
        char pt [] = new char[pat.length() - start];
        Map<Character, Integer> m = new HashMap<Character, Integer>();
        int length = 0;
        int limit = pat.length();
        int n = start;
        boolean hasLetter = false;
        
        For:
        for (; n < limit; ++n)
            if (pat.charAt(n) == escape)
            {
                if (++n < limit)
                    m.put(pt[length++] = ignoreCase && (hasLetter |= Character.isLetter(pat.charAt(n)))
                                                    ? Character.toLowerCase(pat.charAt(n))
                                                    : pat.charAt(n)
                          , n - start);
                else
                    throw new InvalidParameterValueException("Illegal escaped sequence");
            }
            else if (pat.charAt(n) == '%')
            {
                // skip multiples %'s
                do
                    ++n;
                while (n < limit && pat.charAt(n) == '%');

                if (length == 0 && (n < limit || pat.charAt(n -1) != '%'))
                {
                    start = n;
                    --n;
                    continue For;
                }
                else 
                    break For;
            }
            else if (pat.charAt(n) == '_')
                m.put(pt[length++] = '\0', n + 1 - start);
            else
                m.put(pt[length++] = ignoreCase && (hasLetter |= Character.isLetter(pat.charAt(n)))    
                                                ? Character.toLowerCase(pat.charAt(n))
                                                :pat.charAt(n)
                        , n + 1 - start);

        if (length > 0) ret.add(new Token(m, 
                pt, 
                length, 
                ignoreCase && hasLetter,
                n < limit
                    ? false 
                    : !(pat.charAt(n-1) == '%' && ((n - 2 < 0) || pat.charAt(n-2) != escape))));
        return n;
    }
            
    /**
     * Check if a string ends with a given pattern
     * 
     * @param str
     * @param start
     * @param tk
     * @param ignoreCase
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
                    || tk.ignoreCase && tk.pattern[right] == Character.toLowerCase(str.charAt(left))
                    || tk.pattern[right] == '\0')
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
