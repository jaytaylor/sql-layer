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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.io.StringReader;

import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Run basic tests of {@code YamlTester} for YAML files that specify tests that
 * fail during parsing, or that succeed without performing any statement
 * execution.
 */
public class YamlTesterParseTest {

    private static final boolean DEBUG =
	Boolean.getBoolean(YamlTesterParseTest.class.getName() + ".DEBUG");

    /* Test general syntax */

    @Test
    public void testIncorrectYaml() {
	testFail("!!blah-blah", "testIncorrectYaml");
    }

    @Test
    public void testEmptyYaml() {
	testFail("", "testEmptyYaml");
    }

    @Test
    public void testEmptyDocument() {
	testFail("---\n...", "testEmptyDocument");
    }

    @Test
    public void testNotSequence() {
	testFail("a: b\n", "testNotSequence");
    }

    @Test
    public void testFirstElementNotMap() {
	testFail("- a\n", "testNotSequence");
    }

    @Test
    public void testFirstElementKeyNotString() {
	testFail("- 1\n", "testFirstElementKeyNotString");
	testFail("- [a]\n", "testFirstElementKeyNotString");
    }

    @Test
    public void testFirstElementKeyNotKnown() {
	testFail("- UnknownCommand:", "testFirstElementKeyNotKnown");
    }

    /* Test Include */

    @Test
    public void testIncludeMissingValue() {
	testFail("- Include:", "testIncludeMissingValue");
    }

    @Test
    public void testIncludeWrongTypeValue() {
	testFail("- Include: [a, b]",
		 "testIncludeWrongTypeValue");
    }

    @Test
    public void testIncludeFileNotFound() {
	testFail("- Include: file-definitely-not-found",
		 "testIncludeFileNotFound");
    }

    @Test
    public void testIncludeUnexpectedAttributes() throws Exception {
	File include = File.createTempFile("include", null);
	include.deleteOnExit();
	testFail("- Include: " + include +
		 "\n- foo: bar",
		 "testIncludeUnexpectedAttributes");
    }

    // Test nested includes
    // Test relative pathname parsing

    @Test
    public void testIncludeProperties() throws Exception {
	File include = File.createTempFile("include", null);
	include.deleteOnExit();
	writeFile(include,
		  "---\n" +
		  "- Properties: all\n");
	testPass("- Include: " + include);
    }

    /* Test Properties */

    /* Test Statement */

    @Test
    public void testStatementValueInteger() {
	testFail("- Statement: 3", "testStatementValueInteger");
    }

    @Test
    public void testStatementValueSequence() {
	testFail("- Statement: [a, b, c]", "testStatementValueInteger");
    }

    @Test
    public void testStatementUnknownAttribute() {
	testFail("- Statement: a b c" +
		 "\n- unknown_attrib: x y z", "testStatementValueInteger");
    }

    @Test
    public void testStatementParamsNoValue() {
	testFail("- Statement: a b c" +
		 "\n- params:",
		 "testStatementParamsNoValue");
    }

    @Test
    public void testStatementParamsNotSequence() {
	testFail("- Statement: a b c" +
		 "\n- params: 3",
		 "testStatementParamsNotSequence");
    }

    @Test
    public void testStatementParamsValueSequenceOfMaps() {
	testFail("- Statement: a b c" +
		 "\n- params: [a: b]",
		 "testStatementParamsValueSequenceOfMaps");
    }

    @Test
    public void testStatementParamsValueDifferentLengthParams() {
	testFail("- Statement: a b c" +
		 "\n- params: [[a, b], [c, d, e]]",
		 "testStatementParamsValueDifferentLengthParams");
    }

    @Test
    public void testStatementParamsTypesUnknown() {
	testFail("- Statement: a b c" +
		 "\n- params: [[a, b]]" +
		 "\n- param_types: [CHAR, WHATTYPE]",
		 "testStatementParamsTypesUnknown");
    }

    /* Other methods */

    private void testPass(String yaml) {
	new YamlTester(null, new StringReader(yaml), null).test();
    }

    private void testFail(String yaml, String testMethod) {
	try {
	    new YamlTester(null, new StringReader(yaml), null).test();
	} catch (Throwable t) {
	    if (DEBUG) {
		System.err.println(testMethod + ": " + t);
		//t.printStackTrace();
	    }
	    return;
	}
	fail("Expected exception");
    }

    private void writeFile(File file, String text) throws IOException {
	Writer out = new FileWriter(file);
	try {
	    out.write(text);
	} finally {
	    out.close();
	}
    }
}
