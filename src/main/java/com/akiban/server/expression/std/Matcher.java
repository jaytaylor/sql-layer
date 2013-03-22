
package com.akiban.server.expression.std;

public interface Matcher
{
    boolean sameState(String pattern, char escape);

    /**
     * 
     * @param str
     * @param count
     * @return
     *      <p> a negative value if the pattern is not in <code>str</code></p>
     *      <p> a positive number indicating the index at which the pattern/substring is found</p>
     * 
     * Note: Dependent upon the implementation, it's not guaranteed that 
     * the positive number returned by this function is always the index position.
     * The positive value could simply be used as an indication that a match has been found
     */
    int match(String str, int count);
}