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

package com.akiban.util;

import java.io.StringReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySqlStatementSplitterTest {
    private final List<String> inputs = new LinkedList<String>();
    private final List<String> results = new LinkedList<String>();
    private String convertNewlines = "<NL>";
    private boolean keepSpecialComments;
    private boolean preTrim;
    private String lookForPrefix;
    private MySqlStatementSplitter.ReadWriteHook hook;

    @Before
    public void setUp() {
        keepSpecialComments = true;
        preTrim = false;
        lookForPrefix = null;
        hook = null;
    }

    @After
    public void tearDown() {
        testIt();
    }

    @Test
    public void twoStatementsOneLine() {
        inputs.add("one;two;incomplete");

        results.add("one;");
        results.add("two;");
    }

    @Test
    public void simpleMultiline() {
        inputs.add("one;");
        inputs.add("");
        inputs.add("two;");

        convertNewlines = null;

        results.add("one;");
        results.add("\n\ntwo;");
    }

    @Test
    public void comments() {
        inputs.add("comment1;#foo");
        inputs.add("comment2;-- foo;");
        inputs.add("comment3;--foo;"); // this is not "technically" allowed (no
                                       // space after --), but it's used in
                                       // mysql dumps
        inputs.add("comment4;--");
        inputs.add("done;");
        inputs.add("");
        inputs.add("-- comment5");
        inputs.add("-- comment6");
        inputs.add("last line;");

        results.add("comment1;");
        results.add("<NL>comment2;");
        results.add("<NL>comment3;");
        // results.add("--foo;");
        results.add("<NL>comment4;");
        results.add("<NL>done;");
        results.add("<NL><NL><NL><NL>last line;");
    }

    @Test
    public void commentsAlmost() {
        inputs.add("comment1;- foo;");
        inputs.add("comment2;/ bar;");

        results.add("comment1;");
        results.add("- foo;");
        results.add("<NL>comment2;");
        results.add("/ bar;");

    }

    @Test
    public void commentCstyle() {
        inputs.add("comment1;/* comment2 */comment3;");
        inputs.add("comment4;/* comment5 **/comment6;");
        inputs.add("comment7;/** comment8 */comment9;");

        results.add("comment1;");
        results.add("comment3;");
        results.add("<NL>comment4;");
        results.add("comment6;");
        results.add("<NL>comment7;");
        results.add("comment9;");
    }

    @Test
    public void commentCstyleSpecial() {
        inputs.add("comment;/*! special **/comment3;");
        inputs.add("comment;/*! special 'with quotes' **/comment3;");

        results.add("comment;");
        results.add("/*! special **/comment3;");
        results.add("<NL>comment;");
        results.add("/*! special 'with quotes' **/comment3;");
    }

    @Test
    public void quotesSimple() {
        inputs.add("quote 'single';");
        inputs.add("quote \"double\";");
        inputs.add("quote `back`;");

        results.add("quote 'single';");
        results.add("<NL>quote \"double\";");
        results.add("<NL>quote `back`;");
    }

    @Test
    public void quoteEscaped() {
        inputs.add("single 'one''two`\"three';");
        inputs.add("double \"one\"\"two`'three'\";");
        inputs.add("back `one``two'\"three`;");

        results.add("single 'one''two`\"three';");
        results.add("<NL>double \"one\"\"two`'three'\";");
        results.add("<NL>back `one``two'\"three`;");
    }

    @Test
    public void quotesEscapedBySlash() {
        inputs.add("hello \"\\\" dolly\"\\\\;");
        inputs.add("hello '\\' dolly'\\\\;");

        results.add("hello \"\\\" dolly\"\\\\;");
        results.add("<NL>hello '\\' dolly'\\\\;");
    }

    @Test
    public void quotesEscapedOutsideOfQuote()
    {
        inputs.add("hello \\\" dolly\\\\;");
        inputs.add("hello \\' dolly\\\\;");
        
        results.add("hello \\\" dolly\\\\;");
        results.add("<NL>hello \\' dolly\\\\;");
    }

    @Test
    public void quotesTricky() {
        inputs.add("multiline quote `one");
        inputs.add("two` done;");
        inputs.add("quote 'contains # comment';");
        inputs.add("quote 'contains -- comment';");
        inputs.add("quote 'contains /* a multi-");
        inputs.add("line */ comment';");

        results.add("multiline quote `one\ntwo` done;");
        results.add("<NL>quote 'contains # comment';");
        results.add("<NL>quote 'contains -- comment';");
        results.add("<NL>quote 'contains /* a multi-\nline */ comment';");
    }

    @Test
    public void newlines1() {
        inputs.add("multi;");
        inputs.add("line;");
        inputs.add("\rnr-order;");
        inputs.add("rn-order1;\r");
        inputs.add("rn-order2;");

        inputs.add("multiline/* one");
        inputs.add("two*/ comment;");

        results.add("multi;");
        results.add("<NL>line;");
        results.add("<NL>nr-order;");
        results.add("<NL>rn-order1;");
        results.add("<NL>rn-order2;");
        results.add("<NL>multiline comment;");
    }

    @Test
    public void dontKeepSpecialComments() {
        inputs.add("special 1 /*! one line */;");
        inputs.add("special 2 /*! multi");
        inputs.add("line */;");

        results.add("special 1 ;");
        results.add("<NL>special 2 ;");

        keepSpecialComments = false;
    }

    @Test
    public void pretrim() {
        inputs.add("  \tone;");
        inputs.add("\t\ttwo;");
        inputs.add("three;");
        inputs.add("");
        inputs.add("`four`;");
        inputs.add(" ");
        inputs.add("");
        inputs.add(" five;'");
        inputs.add(" six';");
        inputs.add("seven /*! 7.5");
        inputs.add(" */ eight;");
        inputs.add("     ;");

        results.add("one;");
        results.add("two;");
        results.add("three;");
        results.add("`four`;");
        results.add("five;");
        results.add("'\n six';");
        results.add("seven /*! 7.5\n */ eight;");
        results.add(";");

        preTrim = true;
    }

    @Test
    public void matchPrefix() {
        inputs.add("foo ONE;");
        inputs.add("   foo TWO;");
        inputs.add("FOOTHREE;");
        inputs.add("barFIVE;");
        inputs.add(" barSIX;");
        inputs.add("fOoSEVEN;FOOEIGHT;");
        inputs.add("/* comment 1 */FOO NINE;");
        inputs.add("'foo' TEN;");
        inputs.add("/*! ELEVEN */ FOO;FOO;");

        results.add("foo ONE;");
        results.add("<NL>   foo TWO;");
        results.add("<NL>FOOTHREE;");
        results.add("<NL>fOoSEVEN;");
        results.add("FOOEIGHT;");
        results.add("<NL>FOO NINE;");
        results.add("FOO;");

        lookForPrefix = "foo";
    }

    @Test
    public void testHook() {
        // Create a hook that will turn every char we see into an at sign.
        hook = new MySqlStatementSplitter.ReadWriteHook() {
            @Override
            public void seeChar(StringBuilder builder) {
                builder.setCharAt(builder.length()-1, '@');
            }
        };
        inputs.add("foo ONE;");
        inputs.add("   TWO;");
        inputs.add("Three@;# four five;");
        inputs.add("six/*seven*/eight;");
        inputs.add("nine;-- ten");
        inputs.add("eleven 'twelve\nthirteen';");
        inputs.add("fourteen/*! fifteen */sixteen;");

        results.add(    "@@@@@@@;");
        results.add("<NL>@@@@@@;");
        results.add("<NL>@@@@@@;");
        results.add("<NL>@@@@@@@@;");
        results.add("<NL>@@@@;");
        results.add("<NL>@@@@@@@'twelve\nthirteen';");
        results.add("<NL>@@@@@@@@/*! fifteen */@@@@@@@;");
    }

    @Test
    public void matchPrefixEmpty() {
        inputs.add("one;");

        results.add("one;");

        lookForPrefix = "";
    }

    private void testIt() {
        final String inputsString;
        {
            StringBuilder inputsBuilder = new StringBuilder();
            for (String input : inputs) {
                inputsBuilder.append(input).append('\n');
            }
            int len = inputsBuilder.length();
            if (len > 0) {
                inputsBuilder.setLength(len - 1);
            }
            inputsString = inputsBuilder.toString();
        }

        String[] expecteds = results.toArray(new String[results.size()]);
        MySqlStatementSplitter sqlReader = new MySqlStatementSplitter(
                new StringReader(inputsString), convertNewlines,
                keepSpecialComments, preTrim, lookForPrefix);
        sqlReader.setHook(hook);
        List<String> actuals = sqlReader.asList();


        Assert.assertEquals("values", Arrays.asList(expecteds), actuals);
    }
}
