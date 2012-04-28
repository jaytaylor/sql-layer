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
        
        // store all the positions such that the substring starting at this toward the end
        // completely matches the substring starting from 0
        final TreeSet<Integer> circular;
        
        Token (Map<Character, Integer> p, char pat[], int len, boolean ic, boolean end, TreeSet<Integer> cir)
        {
            pos = p;
            pattern = pat;
            length = len;
            ignoreCase = ic;
            endsWith = end;
            circular = cir;
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
    
    static abstract class AbstractMatcher implements Matcher
    {    
        // global variable, marking the position in the text string
        // should be altered in contain(...)
        protected int nextStart;
        protected boolean contain (String str, int start, int limit, Token tk)
        {
            char ch;
            int left = start, right = 0, lastPos = tk.length - 1;
            int tail;
            Integer d;
            
            While:
            while (left < limit)
            {
                if ((tail = left + lastPos) >= limit)
                    return false; // ??? pattern is longer than text???
                
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
                                left += right - d;      // greater than right (to prevent negative shifting)
                            else // if there's no such char in pattern
                            {    
                                // try to match the prefix of pattern starting from [0, right-1] with the suffix
                                TreeSet<Integer> w = tk.circular;
                                if (w != null && (d = w.ceiling(right)) != null)
                                    left += d;
                                else
                                    left += tk.length;
                            }
                            continue While;
                        }
                    }
                    return true; // if someting has gone wrong, we would've skipped it
                }
                else
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
    
    /**
     * the pattern starts with percent
     */
    static class Contain extends AbstractMatcher
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
            
            do
                start = computePos(escape, pattern, start, tokens, ignoreCase);
            while (start < pattern.length());
        }
        
        @Override
        public boolean match(String str)
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
                        return false;
                    r += 2;
                    ++l;
                }
                else if (rch == '%')
                {
                    if (r + 1 == r_len) // end of string
                        return true;
                    
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
                        return false;
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
        char ch;
        
        int length = 0;
        int limit = pat.length();
        int n = start;
        Integer pos;
        
        boolean hasLetter = false;
        
        // map each character to its right most position
        Map<Character, Integer> shift = new HashMap<Character, Integer>();
        
        // positions at which the first char appears
        List<Integer> occurences = new LinkedList<Integer>();
        
        // store all the positions such that the substring starting at this toward the end
        // completely matches the substring starting from 0
        TreeSet<Integer> wrap;

        For:
        for (; n < limit; ++n)
        {
            if ((ch = pat.charAt(n)) == escape)
            {
                if (++n < limit)
                {
                    ch = pat.charAt(n);
                    pos = shift.put(pt[length] = ignoreCase && (hasLetter |= Character.isLetter(ch))
                                                    ? Character.toLowerCase(ch)
                                                    : ch
                          , length);
                  
                    if (pos != null && (ch == pt[0]))
                        occurences.add(length);
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
                while (n < limit && pat.charAt(n) == '%');

                if (length == 0 && (n < limit || pat.charAt(n -1) != '%'))
                {
                    --n;
                    continue For;
                }
                else 
                    break For;
            }
            else if (ch == '_')
            {
                pos = shift.put(pt[length] = '\0', length);
                if (pos != null && pt[0] == '\0')
                    occurences.add(length);
                ++length;
            }
            else
            {
               pos =  shift.put(pt[length] = ignoreCase && (hasLetter |= Character.isLetter(ch))    
                                                ? Character.toLowerCase(ch)
                                                : ch
                        , length);
                                    
                if (pos != null && ch == pt[0])
                        occurences.add(length);
                ++length;
            }
        }
        
        // find the suffix that matches the prefix (the wrap-around region)
        wrap = new TreeSet<Integer>();     
        for  (Integer p : occurences)
        {
            int bound = length - p;
            for (int  i = 0; i < bound; ++i)
                if (pt[i] != pt[i + p])
                    continue;
            wrap.add(p);
        }

        if (length > 0) ret.add(new Token(shift, 
                pt, 
                length, 
                ignoreCase && hasLetter,
                n < limit
                    ? false 
                    : !(pat.charAt(n-1) == '%' && ((n - 2 < 0) || pat.charAt(n-2) != escape)),
                wrap.isEmpty() ? null : wrap));
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
