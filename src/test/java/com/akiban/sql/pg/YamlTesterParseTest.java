/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import java.io.StringReader;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Run basic tests of {@code YamlTester} for YAML files that specify tests that
 * fail during parsing.
 */
public class YamlTesterParseTest {

    @Test
    public void testIncorrectYaml() {
	test("!!blah-blah", "testIncorrectYaml");
    }

    @Test
    public void testEmptyYaml() {
	test("", "testEmptyYaml");
    }

    @Test
    public void testEmptyDocument() {
	test("---\n...", "testEmptyDocument");
    }

    @Test
    public void testNotSequence() {
	test("a: b\n", "testNotSequence");
    }

    @Test
    public void testFirstElementNotMap() {
	test("- a\n", "testNotSequence");
    }

    @Test
    public void testFirstElementKeyNotString() {
	test("- 1\n", "testFirstElementKeyNotString");
	test("- [a]\n", "testFirstElementKeyNotString");
    }

    @Test
    public void testFirstElementKeyNotKnown() {
	test("- UnknownCommand:", "testFirstElementKeyNotKnown");
    }

    @Test
    public void testStatementValueInteger() {
	test("- Statement: 3", "testStatementValueInteger");
    }

    @Test
    public void testStatementValueSequence() {
	test("- Statement: [a, b, c]", "testStatementValueInteger");
    }

    @Test
    public void testStatementUnknownAttribute() {
	test("- Statement: a b c" +
	     "\n- unknown_attrib: x y z", "testStatementValueInteger");
    }

    @Test
    public void testStatementParamsNoValue() {
	test("- Statement: a b c" +
	     "\n- params:",
	     "testStatementParamsNoValue");
    }

    @Test
    public void testStatementParamsNotSequence() {
	test("- Statement: a b c" +
	     "\n- params: 3",
	     "testStatementParamsNotSequence");
    }

    @Test
    public void testStatementParamsValueSequenceOfMaps() {
	test("- Statement: a b c" +
	     "\n- params: [a: b]",
	     "testStatementParamsValueSequenceOfMaps");
    }

    @Test
    public void testStatementParamsValueDifferentLengthParams() {
	test("- Statement: a b c" +
	     "\n- params: [[a, b], [c, d, e]]",
	     "testStatementParamsValueDifferentLengthParams");
    }

    private void test(String yaml, String testMethod) {
	try {
	    new YamlTester(null, new StringReader(yaml), null).test();
	} catch (Throwable t) {
	    System.out.println(testMethod + ": " + t);
	    return;
	}
	fail("Expected exception");
    }
}
