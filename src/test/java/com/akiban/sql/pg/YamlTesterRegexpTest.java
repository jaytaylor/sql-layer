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

package com.akiban.sql.pg;

import com.akiban.sql.pg.YamlTester.Regexp;

import static org.junit.Assert.fail;
import org.junit.Test;

/** Test the YamlTester.Regexp class. */

public class YamlTesterRegexpTest {

    @Test(expected=NullPointerException.class)
    public void testConstructorNull() {
	new Regexp(null);
    }

    @Test
    public void testSimple() {
	test("", "", true);
	test("null", "null", true);
	test("null", null, true);
	test("abc", "abc", true);
	test("abc", "ab", false);
	test("abc", "abcd", false);
	test("1", new Integer(1), true);
	test("1", new Integer(0), false);
	test("1.0", new Double(1.0), true);
	test("1.0", new Double(-1.0), false);
    }

    @Test
    public void testFeatures() {
	test("ab\\\\c", "ab\\c", true);
	test("a.c", "axc", true);
	test("a[bc]d", "abd", true);
	test("a[bc]d", "acd", true);
	test("a[bc]d", "aed", false);
	test("(ab|cd)ef", "abef", true);
	test("(ab|cd)ef", "cdef", true);
	test("(ab|cd)ef", "adef", false);
	test("ab*c", "ac", true);
	test("ab*c", "abc", true);
	test("ab*c", "abbbc", true);
	test("ab*c", "aabc", false);
	test("ab?c", "ac", true);
	test("ab?c", "abc", true);
	test("ab?c", "abbc", false);
	test("ab+c", "ac", false);
	test("ab+c", "abc", true);
	test("ab+c", "abbbbc", true);
	test("ab+c", "abbbbcc", false);
    }

    @Test
    public void testCaptureReference() {
	test("a b c \\{d e f\\}", "a b c {d e f}", true);
	test("([ab]+)([cd]+){1}{2}", "badcbadc", true);
	test("([ab]+)([cd]+){1}{2}", "badcabcd", false);
	test("\\{abc\\}", "{abc}", true);
	test("\\{123\\}", "{123}", true);
    }

    private static void test(String pattern, Object output, boolean match) {
	boolean result = new Regexp(pattern).compareExpected(output);
	if (result != match) {
	    fail("Expected pattern '" + pattern + "' and output '" + output +
		 "' to " + (match ? "" : "not ") + "match");
	}
    }
}
