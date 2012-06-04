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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubStringIndexExpression extends AbstractTernaryExpression {
    
    @Scalar("substring_index")
    public final static ExpressionComposer COMPOSER = new TernaryComposer() 
    {

        @Override
        protected Expression compose(Expression first, Expression second, Expression third)
        {
            return new SubStringIndexExpression(first, second, third);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3) 
                throw new WrongExpressionArityException(3, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.VARCHAR);
            argumentTypes.setType(2, AkType.INT);
            int length = argumentTypes.get(0).getPrecision() +
                    argumentTypes.get(1).getPrecision() + 
                    argumentTypes.get(2).getPrecision();
            
            return ExpressionTypes.varchar(length);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            throw new UnsupportedOperationException("Not supported in SUBSTRING_INDEX yet.");
        }
        
    };
    
    private static final class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        public InnerEvaluation(List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }
        
        // Uses modified Boyer-Moore search algorithm
        @Override
        public ValueSource eval()
        {
            ValueSource first = this.children().get(0).eval();
            ValueSource second = this.children().get(1).eval();
            ValueSource third = this.children().get(2).eval();
            if (first.isNull() || second.isNull() || third.isNull())
                return NullValueSource.only();
            
            String string = first.getString();
            String pattern = second.getString();
            int count = (int) third.getInt();
            boolean signed = count < 0;

            int indexOf = indexOf(string.toCharArray(), pattern.toCharArray(), count);
            String substring = signed ? string.substring(indexOf, string.length()) : string.substring(0, indexOf);
            
            valueHolder().putRaw(AkType.VARCHAR, substring);
            return valueHolder();
        }
        
        private static int indexOf(char[] haystack, char[] needle, int count) 
        {
            boolean signed = count < 0;
 
            if (needle.length == 0 || count == 0) return 0;
            
            Map<Character,Integer> charMap = makeCharTable(needle, signed);
            int offsetTable[] = signed ? negOffsetTable(needle) : makeOffsetTable(needle);
            return signed ? negIndexOf(haystack, needle, count, charMap, offsetTable) :
                    posIndexOf(haystack, needle, count, charMap, offsetTable);
        }
        
        private static int posIndexOf(char[] haystack, char[] needle, int count, 
                Map<Character,Integer> charMap, int[] offsetTable)
        {
            int haystack_len = haystack.length;
            int needle_len = needle.length;
            for (int i = needle_len - 1, j; i < haystack_len;)
            {
                for (j = needle_len - 1; needle[j] == haystack[i]; --i,--j)
                {
                    if (j == 0) 
                    {
                        if (count == 1) return i;
                        count--;
                        break;
                    }
                }
                int max = (charMap.containsKey(haystack[i])) ? 
                        Math.max(offsetTable[needle_len-1-j], charMap.get(haystack[i])) :
                        Math.max(offsetTable[needle_len-1-j], needle_len);
                i += max;
            }
            return haystack_len;
        }
        
        private static int negIndexOf(char[] haystack, char[] needle, int count, 
                Map<Character,Integer> charMap, int[] offsetTable)
        {
            int haystack_len = haystack.length;
            int needle_len = needle.length;
            for (int i = haystack_len-needle_len, j; i < haystack_len && i >= 0;)
            {
                for (j = 0; needle[j] == haystack[i]; ++i, ++j)
                {
                    if (j == needle_len-1)
                    {
                        if (count == -1) return i + 1;
                        count++;
                        break;
                    }
                }
                int max = charMap.containsKey(haystack[i]) && charMap.get(haystack[i]) > 0 ?
                        charMap.get(haystack[i]) : Math.max(offsetTable[j], needle_len);

                i -= max;
            }
            return 0;
        }
        
        private static Map<Character,Integer> makeCharTable(char[] needle, boolean signed)
        {
            int needle_len = needle.length;
            Map<Character,Integer> map = new HashMap<Character, Integer>(needle_len);
            if (signed)
            {
                for (int i = needle_len - 1; i >= 0; i--)
                {
                    map.put(needle[i], i);
                }
            }
            else
            {
                for (int i = 0; i < needle_len - 1; ++i)
                {
                    map.put(needle[i], needle_len - 1 - i);
                
                }
            }       
            return map;
        }
        
        private static int[] makeOffsetTable(char[] needle) 
        {
            int needle_len = needle.length;
            int[] table = new int[needle_len];
            int lastPrefixPosition = needle_len;
            
            for (int i = needle_len - 1; i >= 0; --i)
            {
                if (isPrefix(needle, i+1)) lastPrefixPosition = i + 1;
                table[needle_len-1-i] = lastPrefixPosition - i + needle_len - 1;
            }
            for (int i = 0; i < needle_len - 1; ++i)
            {
                int slen = suffixLength(needle, i);
                table[slen] = needle_len - 1 - i + slen;
            }
            return table;
        }
        
        private static int[] negOffsetTable(char[] needle)
        {
            int needle_len = needle.length;
            int [] table = new int[needle_len];
            int lastSuffixPosition = -1;
            
            for (int i = 0; i < needle_len; ++i)
            {
                if (isSuffix(needle, i-1)) lastSuffixPosition = i - 1;
                table[i] = lastSuffixPosition + i + needle_len - 1;
            }
            for (int i = needle_len - 1; i > 0; --i)
            {
                int plen = prefixLength(needle, i);
                table[plen] = needle_len - 1 - i + plen;
            }
            return table;
        }
        
        private static boolean isSuffix(char[] needle, int p)
        {
            int needle_len = needle.length;
            for (int i = p, j = needle_len - 1; i >= 0; --i, --j)
            {
                if (needle[i] != needle[j]) return false;
            }
            return true;            
        }
        
        private static boolean isPrefix(char[] needle, int p)
        {
            int needle_len = needle.length;
            for (int i = p, j = 0; i < needle_len; ++i, ++j)
            {
                if (needle[i] != needle[j]) return false;
            }
            return true;
        }
        
        private static int suffixLength (char[] needle, int p) 
        {
            int needle_len = needle.length;
            int len = 0;
            for (int i = p, j = needle_len - 1;
                    i >= 0 && needle[i] == needle[j]; --i, --j)
            {
                len += 1;
            }
            return len;
        }
        
        private static int prefixLength (char[] needle, int p)
        {
            int needle_len = needle.length;
            int len = 0;
            for (int i = p, j = 0; i < needle_len && 
                    needle[i] == needle[j]; ++i, ++j)
            {
                len += 1;
            }
            return len;
        }
        
    }
    
    public SubStringIndexExpression(Expression first, Expression second, Expression third) 
    {
        super(AkType.VARCHAR, first, second, third);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("SUBSTRING_INDEX");
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }

}
