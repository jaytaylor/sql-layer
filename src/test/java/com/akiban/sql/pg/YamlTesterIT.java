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

import com.akiban.server.types.extract.ConverterTestUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;

import static org.junit.Assert.fail;
import org.junit.Test;

/** Test the {@code YamlTester} class. */
public class YamlTesterIT extends PostgresServerYamlITBase {

    static
    {
        ConverterTestUtils.setGlobalTimezone("UTC");
    }
    /* Tests */

    /* Test general syntax */

    @Test
    public void testIncorrectYaml() {
	testYamlFail("!!blah-blah");
    }

    @Test
    public void testEmptyYaml() {
	testYamlFail("");
    }

    @Test
    public void testEmptyDocument() {
	testYamlFail("---\n...");
    }

    @Test
    public void testNotSequence() {
	testYamlFail("a: b\n");
    }

    @Test
    public void testFirstElementNotMap() {
	testYamlFail("- a\n");
    }

    @Test
    public void testFirstElementKeyNumber() {
	testYamlFail("- 1\n");
    }

    @Test
    public void testFirstElementKeySequence() {
	testYamlFail("- [a]\n");
    }

    @Test
    public void testFirstElementKeyNotKnown() {
	testYamlFail("- UnknownCommand:");
    }

    /* Test !select-engine general syntax */

    @Test
    public void testSelectEngineMissingValue() {
	testYamlFail("- Statement: !select-engine");
    }

    @Test
    public void testSelectEngineScalarValue() {
	testYamlFail("- Statement: !select-engine foo");
    }

    @Test
    public void testSelectEngineSequenceValue() {
	testYamlFail("- Statement: !select-engine [foo, bar]");
    }

    @Test
    public void testSelectEngineKeyNotScalar() {
	testYamlFail("- Statement: !select-engine { [foo, bar]: baz }");
    }

    @Test
    public void testSelectEngineMatch() {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: !select-engine\n" +
		 "    { foo: bar,\n" +
		 "      it: INSERT INTO t VALUES (1) }\n" +
		 "...");
    }

    @Test
    public void testSelectEngineNoMatch() {
	testYaml("- Statement: !select-engine { foo: bar }");
    }

    @Test
    public void testSelectEngineKeyItOverAllPrecedence() {
	testYaml("---\n" +
                 "- CreateTable: t (bigint_field bigint)\n" +
                 "---\n" +
                 "- Statement: !select-engine\n" +
                 "    { it: SELECT * FROM t, all: oops }");
    }

    @Test
    public void testSelectEngineKeyItOverAllPrecedenceReversed() {
	testYaml("---\n" +
                 "- CreateTable: t (bigint_field bigint)\n" +
                 "---\n" +
                 "- Statement: !select-engine\n" +
                 "    { all: oops, it: SELECT * FROM t }");
    }

    @Test
    public void testSelectEngineKeyItOverAllPrecedenceNull() {
	testYaml("---\n" +
                 "- Statement: !select-engine { it: null, all: oops }");
	testYaml("---\n" +
                 "- Statement: !select-engine { all: oops, it: null }");
	testYamlFail("---\n" +
                     "- Statement: !select-engine { it: oops, all: null }");
	testYamlFail("---\n" +
                     "- Statement: !select-engine { all: null, it: oops }");
    }

    @Test
    public void testSelectEngineKeyAll() {
	testYaml("---\n" +
                 "- CreateTable: t (bigint_field bigint)\n" +
                 "---\n" +
                 "- Statement: !select-engine\n" +
                 "    { all: SELECT * FROM t, foo: oops }");
    }

    @Test
    public void testSelectEngineKeyAllNull() {
	testYaml("---\n" +
                 "- Statement: !select-engine { all: null, foo: oops }");
	testYaml("---\n" +
                 "- Statement: !select-engine { foo: oops, all: null }");
    }

    /* Test Include */

    @Test
    public void testIncludeMissingValue() {
	testYaml("- Include:");
    }

    @Test
    public void testIncludeNullValue() {
	testYaml("- Include: null");
    }

    @Test
    public void testIncludeWrongTypeValue() {
	testYamlFail("- Include: [a, b]");
    }

    @Test
    public void testIncludeFileNotFound() {
	testYamlFail("- Include: file-definitely-not-found");
    }

    @Test
    public void testIncludeUnexpectedAttributes() throws Exception {
	testYamlFail("- Include: somefile\n" +
		     "- foo: bar");
    }

    @Test
    public void testIncludeRegexp() throws Exception {
        testYamlFail("- Include: !re foo.yaml");
    }

    @Test
    public void testIncludeSimple() throws Exception {
	File include = File.createTempFile("include", null);
	include.deleteOnExit();
	writeFile(include,
		  "---\n" +
		  "- CreateTable: c (cid int)\n" +
		  "---\n" +
		  "- Statement: INSERT INTO c VALUES (1), (2)\n");
	testYaml("---\n" +
		 "- Include: " + include + "\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM c\n" +
		 "- output: [[1], [2]]\n");
    }

    @Test
    public void testIncludeNested() throws Exception {
	File include1 = File.createTempFile("include1", null);
	include1.deleteOnExit();
	File include2 = File.createTempFile("include2", null);
	include2.deleteOnExit();
	writeFile(include1,
		  "---\n" +
		  "- Include: " + include2 + "\n" +
		  "---\n" +
		  "- Statement: INSERT INTO c VALUES (1), (2)\n");
	writeFile(include2,
		  "---\n" +
		  "- CreateTable: c (cid int)\n");
	testYaml("---\n" +
		 "- Include: " + include1 + "\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM c\n" +
		 "- output: [[1], [2]]\n");
    }

    @Test
    public void testIncludeNestedFileNotFound() throws Exception {
	File include = File.createTempFile("include", null);
	include.deleteOnExit();
	writeFile(include,
		  "---\n" +
		  "- Include: file-that-is-not-found");
	testYamlFail("---\n" +
		     "- Include: " + include);
    }

    @Test
    public void testIncludeNestedRelative() throws Exception {
	File include1 = File.createTempFile("include1", null);
	include1.deleteOnExit();
	File include2 = File.createTempFile("include2", null);
	include2.deleteOnExit();
	writeFile(include1,
		  "---\n" +
		  "- Include: " + include2.getName() + "\n" +
		  "---\n" +
		  "- Statement: INSERT INTO c VALUES (1), (2)\n");
	writeFile(include2,
		  "---\n" +
		  "- CreateTable: c (cid int)\n");
	testYaml("---\n" +
		 "- Include: " + include1 + "\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM c\n" +
		 "- output: [[1], [2]]\n");
    }

    @Test
    public void testIncludeHasError() throws Exception {
	File include = File.createTempFile("include", null);
	include.deleteOnExit();
	writeFile(include,
		  "---\n" +
		  "- Properties: all\n" +
		  "- prop_b: prop_b_value\n" +
		  "---\n" +
		  "- Statement: a b c\n" +
		  "- unexpected_attribute: nope");
	testYamlFail("---\n" +
		     "- Properties: all\n" +
		     "- prop_a: prop_a_value\n" +
		     "---\n" +
		     "- Include: " + include);
    }

    @Test
    public void testIncludeSelectEngine() throws Exception {
	File includeFile = File.createTempFile("include", null);
	includeFile.deleteOnExit();
	writeFile(includeFile, "- CreateTable: t (bigint_field bigint)\n");
        /*
         * Double backslashes, to escape them in the YAML format.  Note that
         * they need to be doubled for Java strings, and double again for
         * regexps.
         */
        String include = includeFile.getPath().replaceAll("\\\\", "\\\\");
	testYaml("---\n" +
		 "- Include: !select-engine { foo: bar, it: '" + include +
		 "' }\n" +
		 "---\n" +
		 "- Statement: SELECT bigint_field FROM t\n" +
		 "...");
    }

    /* Test Properties */

    @Test
    public void testPropertiesNoValue() {
	testYamlFail("---\n" +
		     "- Properties:\n");
    }

    @Test
    public void testPropertiesNullValue() {
	testYamlFail("---\n" +
		     "- Properties: null\n");
    }

    @Test
    public void testPropertiesValueNotString() {
	testYamlFail("---\n" +
		     "- Properties: 33\n");
    }

    @Test
    public void testPropertiesValueUnknownEngine() {
	testYaml("---\n" +
		 "- Properties: gorp\n");
    }

    @Test
    public void testPropertiesValueRegexp() {
        testYamlFail("---\n" +
                     "- Properties: !re all\n");
    }

    @Test
    public void testPropertiesValueAttributesNotSequenceOfMaps() {
	testYamlFail("---\n" +
		     "- Properties: all\n" +
		     "33\n");
    }

    @Test
    public void testPropertiesValueSuppressedNoValue() {
	testYamlFail("---\n" +
		     "- Properties: all\n" +
		     "- suppressed:\n");
    }

    @Test
    public void testPropertiesValueSuppressedBadValue() {
	testYamlFail("---\n" +
		     "- Properties: all\n" +
		     "- suppressed: gorp\n");
    }

    @Test
    public void testPropertiesValueSuppressedAll() {
	testYaml("---\n" +
		 "- Properties: all\n" +
		 "- suppressed: true\n" +
		 "---\n" +
		 "- Never seen!\n");
    }

    @Test
    public void testPropertiesValueSuppressedIt() {
	testYaml("---\n" +
		 "- Properties: it\n" +
		 "- suppressed: true\n" +
		 "---\n" +
		 "- Never seen!\n");
    }

    /* Test CreateTable */

    @Test
    public void testCreateTableNoValue() {
	testYamlFail("- CreateTable:");
    }

    @Test
    public void testCreateTableNullValue() {
	testYamlFail("- CreateTable: null");
    }

    @Test
    public void testCreateTableSequenceValue() {
	testYamlFail("- CreateTable: [a, b, c]");
    }

    @Test
    public void testCreateTableUnexpectedAttribute() {
	testYamlFail("- CreateTable: foo (int_field int)\n" +
		     "- unexpected_attribute: quux");
    }

    @Test
    public void testCreateTableSuccess() {
	testYaml("- CreateTable: foo (int_field int)");
    }

    /* Test CreateTable error */

    @Test
    public void testCreateTableErrorNoValue() {
	testYamlFail(
	    "- CreateTable: a b c\n" +
	    "- error:");
    }

    @Test
    public void testCreateTableErrorNotSequence() {
	testYamlFail(
	    "- CreateTable: a b c\n" +
	    "- error: fooey");
    }

    @Test
    public void testCreateTableErrorEmptySequence() {
	testYamlFail(
	    "- CreateTable: a b c\n" +
	    "- error: []");
    }

    @Test
    public void testCreateTableErrorRepeated() {
        testYamlFail("- CreateTable: a (i int\n" +
                     "- error: [42000]\n" +
                     "- error: [42000]\n");
    }

    @Test
    public void testCreateTableErrorSequenceTooLong() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a b c\n" +
	    "- error: [1, a, b]");
    }

    @Test
    public void testCreateTableErrorCodeNotScalar() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a b c\n" +
	    "- error: [[x, y], a, b]");
    }

    @Test
    public void testCreateTableErrorNotError() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a (i int)\n" +
	    "- error: [1]");
    }

    @Test
    public void testCreateTableNotErrorError() {
	testYamlFail("- CreateTable: a (int_field int");
    }

    @Test
    public void testCreateTableErrorWrongError() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: [37]");
    }

    @Test
    public void testCreateTableErrorRightError() {
	testYaml(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: [42000]");
    }

    @Test
    public void testCreateTableErrorWrongErrorMessage() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: [42000, nope]");
    }

    @Test
    public void testCreateTableErrorRightErrorMessage() {
	testYaml(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error:\n" +
	    "  - 42000\n" +
	    "  - |\n" +
	    "    ERROR: Encountered \" <IDENTIFIER> \"FR \"\" at line 1, column 10.\n" +
	    "    Was expecting one of:\n" +
	    "        \"from\" ...\n" +
	    "        \",\" ...\n" +
	    "        \n" +
	    "      Position: 10");
    }

    @Test
    public void testCreateTableErrorSelectEngine() {
	testYaml(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: !select-engine { it: [42000], sys-aksql: [33] }");
    }

    /* Test Statement */

    @Test
    public void testStatementNoValue() {
	testYaml("- Statement:");
    }

    @Test
    public void testStatementNullValue() {
	testYaml("- Statement: null");
    }

    @Test
    public void testStatementValueInteger() {
	testYamlFail("- Statement: 3");
    }

    @Test
    public void testStatementValueSequence() {
	testYamlFail("- Statement: [a, b, c]");
    }

    @Test
    public void testStatementUnknownAttribute() {
	testYamlFail("- Statement: a b c\n" +
		     "- unknown_attrib: x y z");
    }

    @Test
    public void testStatementCreateTable() {
	testYamlFail("- Statement: create TABLE foo (int_field int)");
    }

    @Test
    public void testStatementSelectEngineMatch() {
	testYaml("- Statement: !select-engine { it: MORP }\n" +
		 "- error: [42000]");
    }

    @Test
    public void testStatementSelectEngineMatchAll() {
	testYaml("- Statement: !select-engine { all: MORP }\n" +
		 "- error: [42000]");
    }

    @Test
    public void testStatementSelectEngineNoMatch() {
	testYaml("- Statement: !select-engine { sys: MORP }");
    }

    /* Test Statement params */

    @Test
    public void testStatementParamsNoValue() {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- params:");
    }

    @Test
    public void testStatementParamsNullValue() {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- params: null");
    }

    @Test
    public void testStatementParamsRepeated() {
        testYamlFail("---\n" +
                     "- CreateTable: t (i int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t WHERE i = ?\n" +
                     "- params: [[1]]\n" +
                     "- params: [[2]]\n");
    }

    @Test
    public void testStatementParamsNotSequence() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: 3");
    }

    @Test
    public void testStatementParamsValueSequenceOfMaps() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [a: b]");
    }

    @Test
    public void testStatementParamsValueEmpty() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: []");
    }

    @Test
    public void testStatementParamsValueEmptySequence() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[], []]");
    }

    @Test
    public void testStatementParamsValueDifferentLengthParams() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[a, b], [c, d, e]]");
    }

    /* Test Statement param_types */

    @Test
    public void testStatementParamsTypesNoParams() {
	testYamlFail("- Statement: a b c\n" +
		     "- param_types: [CHAR]");
    }

    @Test
    public void testStatementParamsTypesNoParamsNoValue() {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- param_types:");
    }

    @Test
    public void testStatementParamsTypesNullParamsNullValue() {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- params: null\n" +
		 "- param_types: null");
    }
 
    @Test
    public void testStatementParamsTypesNoValue() {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t WHERE bigint_field = ?\n" +
		 "- params: [[1]]\n" +
		 "- param_types:");
    }

    @Test
    public void testStatementParamsTypesNullValue() {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t WHERE bigint_field = ?\n" +
		 "- params: [[1]]\n" +
		 "- param_types: null");
    }

    @Test
    public void testStatementParamsTypesUnknown() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[a, b]]\n" +
		     "- param_types: [CHAR, WHATTYPE]");
    }

    @Test
    public void testStatementParamsTypesEmpty() {
	testYamlFail("- Statement: a b c\n" +
		     "- param_types: []");
    }

    @Test
    public void testStatementParamsTypesRepeated() {
        testYamlFail("---\n" +
                     "- CreateTable: t (i int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t WHERE i = ?\n" +
                     "- params: [[1]]\n" +
                     "- param_types: [INTEGER]\n" +
                     "- param_types: [INTEGER]\n");
    }

    @Test
    public void testStatementParamsTypesTooMany() {
	testYamlFail("- Statement: a b c\n" +
		     "- param_types: [CHAR, INTEGER, BOOLEAN]\n" +
		     "- params: [[a, b]]");
    }

    @Test
    public void testStatementParamsTypesTooFew() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[a, b]]\n" +
		     "- param_types: [CHAR]");
    }

    @Test
    public void testStatementParamTypesSuccess() {
	testYaml(
	    "---\n" +
	    "- CreateTable: c (cid int)\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1), (2)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c where cid = ?\n" +
	    "- params: [[1]]\n" +
	    "- param_types: [DECIMAL]\n" +
	    "- output: [[1]]");
    }

    @Test
    public void testStatementParamTypesSelectEngine() {
	testYaml(
	    "---\n" +
	    "- CreateTable: c (cid int)\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1), (2)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c where cid = ?\n" +
	    "- params: [[1]]\n" +
	    "- param_types: !select-engine { it: [DECIMAL], foo: bar }\n" +
	    "- output: [[1]]");
    }

    /* Test Statement output */

    @Test
    public void testStatementOutputNoValue() {
	testYaml("---\n" +
		 "- CreateTable: t (id int)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- output:");
    }

    @Test
    public void testStatementOutputRepeated() {
	testYamlFail("---\n" +
                     "- CreateTable: t (id int)\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES (1)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- output: [[1]]\n" +
                     "- output: [[1]]\n");
    }

    @Test
    public void testStatementOutputNullValue() {
	testYaml("---\n" +
		 "- CreateTable: t (id int)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- output: null");
    }

    @Test
    public void testStatementOutputValueNotSequence() {
	testYamlFail("- Statement: a b c\n" +
		     "- output: 33");
    }

    @Test
    public void testStatementOutputValueNotSequenceOfSequences() {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [33]");
    }

    @Test
    public void testStatementOutputValueNotSequenceOfSequencesOfScalars() {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [[[33]]]");
    }

    @Test
    public void testStatementOutputEmpty() {
	testYamlFail("- Statement: a b c\n" +
		     "- output: []");
    }

    @Test
    public void testStatementOutputEmptyRow() {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [[], []]");
    }

    @Test
    public void testStatementOutputDifferentRowLengths() {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [[a], [b], [c, d, e]]");
    }

    @Test
    public void testStatementOutputWrongResults() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [1, Smith]\n" +
	    "  - [2, Howard]\n");
    }

    @Test
    public void testStatementOutputMissingRow() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [1, Smith]");
    }

    @Test
    public void testStatementOutputExtraRow() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [1, Smith]\n" +
	    "  - [2, Jones]\n" +
	    "  - [2, Jones]\n");
    }

    @Test
    public void testStatementOutputRightResults() {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [1, Smith]\n" +
	    "  - [2, Jones]\n");
    }

    @Test
    public void testStatementOutputEmptyValue() {
        testYaml(
	    "---\n" +
	    "- CreateTable: t (x varchar(32), y varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO t VALUES ('', 'abc')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- output:\n" +
	    "  - ['', 'abc']\n");
    }

    @Test
    public void testStatementOutputDontCare() {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [!dc dc, Smith]\n" +
	    "  - [2, !dc dc]");
    }

    @Test
    public void testStatementOutputDontCareMissingValue() {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [!dc]\n");
    }

    @Test
    public void testStatementOutputRegexp() {
    	testYaml(
    	    "---\n" +
    	    "- CreateTable: t (name varchar(32))\n" +
    	    "---\n" +
    	    "- Statement: INSERT INTO t VALUES\n" +
	    "    ('hubba-hubba'), ('abc123')\n" +
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output:\n" +
    	    "  - [!re '([^-]*)-{1}']\n" +
    	    "  - [!re '[a-z]+[0-9]+']\n");
    }

    @Test
    public void testStatementOutputRegexpMissingValue() {
    	testYamlFail(
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output:\n" +
    	    "  - [!re]\n");
    }

    @Test
    public void testStatementOutputRegexpNonScalarValue() {
    	testYamlFail(
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output:\n" +
    	    "  - [!re [a, b, c]]\n");
    }

    @Test
    public void testStatementOutputRegexpSelectEngine() {
    	testYaml(
    	    "---\n" +
    	    "- CreateTable: t (name varchar(32))\n" +
    	    "---\n" +
    	    "- Statement: INSERT INTO t VALUES\n" +
	    "    ('abc'), ('123')\n" +
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output:\n" +
	    "    !select-engine\n" +
	    "      it: [[!re '[a-z]+'], [!re '[0-9]+']]\n" +
	    "      foo: bar\n");
    }

    @Test
    public void testStatementOutputNullAndEmpty() {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO customers (name)\n" +
	    "    VALUES ('Smith'), (''), ('null')\n" +
	    "---\n" +
	    "- Statement: INSERT INTO customers (cid) VALUES (1)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [null, Smith]\n" +
	    "  - [null, '']\n" +
	    "  - [null, 'null']\n" +
	    "  - [1, null]\n" +
	    "...");
    }

    @Test
    public void testStatementOutputParamsRightResults() {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers WHERE name = ?\n" +
	    "- params: [[Jones], [Smith], [Smith]]\n" +
	    "- output:\n" +
	    "  - [2, Jones]\n" +
	    "  - [1, Smith]\n" +
	    "  - [1, Smith]");
    }

    @Test
    public void testStatementOutputParamsExtraResultsOneCall() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: parts (part varchar(32), product varchar(32))\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO parts VALUES\n" +
	    "    ('a123', 'a'), ('a456', 'a'), ('b789', 'b'), ('b000', 'b'),\n" +
	    "    ('c246', 'c')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM parts WHERE product = ?\n" +
	    "- params: [[a], [b]]\n" +
	    "- output:\n" +
	    "  - [a123, a]\n" +
	    "  - [a456, a]\n" +
	    "  - [b789, b]");
    }

    @Test
    public void testStatementOutputParamsExtraResultsExtraCall() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: parts (part varchar(32), product varchar(32))\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO parts VALUES\n" +
	    "    ('a123', 'a'), ('a456', 'a'), ('b789', 'b'), ('b000', 'b'),\n" +
	    "    ('c246', 'c')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM parts WHERE product = ?\n" +
	    "- params: [[a], [b], [c]]\n" +
	    "- output:\n" +
	    "  - [a123, a]\n" +
	    "  - [a456, a]\n" +
	    "  - [b789, b]\n" +
	    "  - [b000, b]");
    }

    @Test
    public void testStatementOutputParamsExtraOutputOneCall() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: parts (part varchar(32), product varchar(32))\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO parts VALUES\n" +
	    "    ('a123', 'a'), ('a456', 'a'), ('b789', 'b'), ('b000', 'b'),\n" +
	    "    ('c246', 'c')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM parts WHERE product = ?\n" +
	    "- params: [[a], [b]\n" +
	    "- output:\n" +
	    "  - [a123, a]\n" +
	    "  - [a456, a]\n" +
	    "  - [b789, b]\n" +
	    "  - [b000, b]\n" +
	    "  - [c246, c]");
    }

    @Test
    public void testStatementOutputParamsWrongOutput() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: parts (part varchar(32), product varchar(32))\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO parts VALUES\n" +
	    "    ('a123', 'a'), ('a456', 'a'), ('b789', 'b'), ('b000', 'b'),\n" +
	    "    ('c246', 'c')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM parts WHERE product = ?\n" +
	    "- params: [[a], [b]\n" +
	    "- output:\n" +
	    "  - [a123, a]\n" +
	    "  - [a456, a]\n" +
	    "  - [b789, b]\n" +
	    "  - [bxyz, b]");
    }

    /* Test Statement row_count */

    @Test
    public void testStatementRowCountNoValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- row_count:");
    }

    @Test
    public void testStatementRowCountNullValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- row_count: null");
    }

    @Test
    public void testStatementRowCountNotInteger() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- row_count: hello");
    }

    @Test
    public void testStatementRowCountNegative() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- row_count: -33");
    }

    @Test
    public void testStatementRowCountRepeated() {
        testYamlFail("---\n" +
                     "- Statement: a b c\n" +
                     "- row_count: 1\n" +
                     "- row_count: 1\n");
    }

    @Test
    public void testStatementRowCountWrongValue() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "- row_count: 1\n");
    }

    @Test
    public void testStatementRowCountDifferentFromOutput() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output: [[a]]\n" +
	    "- row_count: 2");
    }

    @Test
    public void testStatementRowCountCorrect() {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "- row_count: 2\n");
    }

    @Test
    public void testStatementRowCountSelectEngine() {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "- row_count: !select-engine { foo: 7, all: 2}\n");
    }

    /* Test Statement output_types */

    @Test
    public void testStatementOutputTypesNoValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- output_types:");
    }

    @Test
    public void testStatementOutputTypesRepeated() {
	testYamlFail("---\n" +
                     "- CreateTable: t (i int)\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES (1)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- output_types: [INTEGER]\n" +
                     "- output_types: [INTEGER]\n");
    }

    @Test
    public void testStatementOutputTypesNullValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- output_types: null");
    }

    @Test
    public void testStatementOutputTypesNotSequence() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: 33");
    }

    @Test
    public void testStatementOutputTypesEmpty() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: []");
    }

    @Test
    public void testStatementOutputTypesWrongLengthFromOutput() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: [BOOLEAN]\n" +
	    "- output: [[a, b, c]]");
    }

    @Test
    public void testStatementOutputTypesUnknownType() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: [GROOVY]");
    }

    @Test
    public void testStatementOutputTypesWrongNumberOfTypes() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- output_types: [INTEGER]");
    }

    @Test
    public void testStatementOutputTypesWrongType() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- output_types: [INTEGER, BOOLEAN]");
    }

    @Test
    public void testStatementOutputTypesCorrectTypes() {
	testYaml(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- output_types: [INTEGER, VARCHAR]");
    }

    @Test
    public void testStatementOutputTypesSelectEngine() {
	testYaml(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- output_types: [INTEGER, !select-engine { it: VARCHAR }]");
    }

    /* Test Statement explain */

    @Test
    public void testStatementExplainNoValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- explain:");
    }

    @Test
    public void testStatementExplainRepeated() {
	testYamlFail("---\n" +
                     "- CreateTable: t (i int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- explain: foo\n" +
                     "- explain: bar\n");
    }

    @Test
    public void testStatementExplainNullValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- explain: null");
    }

    @Test
    public void testStatementExplainSequenceValue() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- explain: [d, e, f]");
    }

    @Test
    public void testStatementExplainWrong() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- explain: Nope");
    }

    @Test
    public void testStatementExplainRight() {
	testYaml(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- explain: |\n" +
	    "    project([Field(0), Field(1)])\n" +
	    "      Filter_Default([user.c])\n" +
	    "        GroupScan_Default(full scan on _akiban_c)\n");
    }

    @Test
    public void testStatementExplainSelectEngine() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- explain: !select-engine { all: stuff }");
    }

    /* Test Statement error */

    @Test
    public void testStatementErrorNoValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- error:");
    }

    @Test
    public void testStatementErrorRepeated() {
	testYamlFail("---\n" +
                     "- CreateTable: t (int_field int)\n" +
                     "---\n" +
                     "- Statement: WHAT IS THIS\n" +
                     "- error: [42000]\n" +
                     "- error: [42000]\n");
    }

    @Test
    public void testStatementErrorAndOutput() {
	testYamlFail("---\n" +
                     "- CreateTable: t (int_field int)\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES (1)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- output: [[1]]\n" +
                     "- error: [0]\n");
    }

    @Test
    public void testStatementErrorAndRowCount() {
	testYamlFail("---\n" +
                     "- CreateTable: t (int_field int)\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES (1)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- row_count: 1\n" +
                     "- error: [0]\n");
    }

    @Test
    public void testStatementErrorNullValue() {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- error: null");
    }

    @Test
    public void testStatementErrorNotSequence() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: fooey");
    }

    @Test
    public void testStatementErrorEmptySequence() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: []");
    }

    @Test
    public void testStatementErrorSequenceTooLong() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: [1, a, b]");
    }

    @Test
    public void testStatementErrorCodeNotScalar() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: [[x, y], a, b]");
    }

    @Test
    public void testStatementErrorNotError() {
	testYamlFail(
	    "---\n" +
	    "- CreateTable a (i int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM a\n" +
	    "- error: [1]");
    }

    @Test
    public void testStatementErrorWrongError() {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [37]");
    }

    @Test
    public void testStatementErrorRightError() {
	testYaml(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [42000]");
    }

    @Test
    public void testStatementErrorWrongErrorMessage() {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [42000, nope]");
    }

    @Test
    public void testStatementErrorRightErrorMessage() {
	testYaml(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error:\n" +
	    "  - 42000\n" +
	    "  - |\n" +
	    "    ERROR: Encountered \" <IDENTIFIER> \"FR \"\" at line 1, column 10.\n" +
	    "    Was expecting one of:\n" +
	    "        \"from\" ...\n" +
	    "        \",\" ...\n" +
	    "        \n" +
	    "      Position: 10");
    }

    @Test
    public void testStatementErrorSelectEngineNoMatch() {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: !select-engine { foo: [bar] }");
    }

    @Test
    public void testStatementErrorSelectEngineNoMatchWithMessage() {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [!select-engine { foo: bar }, Hello]");
    }

    @Test
    public void testStatementErrorSelectEngineMatch() {
	testYaml(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [!select-engine { it: 42000, all: 42 }]");
    }
    
    /* Test Statement sorted_output */

    @Test
    public void testSortedOutput() throws Exception {
        testYamlFail(
                "---\n" +
                "- CreateTable: c (cid int, name varchar(32))\n" +
                "---\n" +
                "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones'), (3, 'Zoolander'), (4, 'Adams') \n" +
                "---\n" +
                "- Statement: SELECT * FROM c\n" +
                "- output: [[1, 'Smith'],[2, 'Jones'],[4, 'Adams'],[3, 'Zoolander']]");
        testYaml("---\n" +
                "- Statement: SELECT * FROM c\n" +
                "- output_ordered: [[1, 'Smith'],[2, 'Jones'],[4, 'Adams'],[3, 'Zoolander']]");
        testYaml("---\n" +
                "- Statement: SELECT * FROM c\n" +
                "- output_ordered: [[3, 'Zoolander'],[1, 'Smith'],[2, 'Jones'],[4, 'Adams']]");
        testYamlFail("---\n" +
                "- Statement: SELECT * FROM c\n" +
                "- output_ordered: [[3, 'Zoolander'],[1, 'Wendel'],[2, 'Jones'],[4, 'Adams']]");
        testYamlFail("---\n" +
                "- Statement: SELECT * FROM c order by cid desc \n" +
                "- output: [[3, 'Zoolander'],[1, 'Smith'],[2, 'Jones'],[4, 'Adams']]");
    }

    @Test
    public void testIgnoreBulkloadCommand() {
        testYaml("---\n- Bulkload: /home/akiba/fts_basis.properties\n"+
        "- properties: {'dataset.coi.address.ratio': 3, 'dataset.coi.order.ratio': 2, 'dataset.coi.customer.count': 200, 'dataset.coi.item.ratio': 2}\n...");
    }
    
    /* Test Statement warnings_count */
    @Test
    public void testStatementWarningsCountNoValue() {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings_count:\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountRepeated() {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 1\n" +
                     "- warnings_count: 2\n");
    }

    @Test
    public void testStatementWarningsCountNullValue() {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings_count: null\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountString() {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 'abc'\n" +
                     "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountWrongValue() {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 1\n" +
                     "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountDifferentFromWarnings() {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 2\n" +
                     "- warnings: [[ 'abc' ]]\n" +
                     "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountNonZeroNoWarnings() {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 1\n");
    }

    @Test
    public void testStatementWarningsCountZero() {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountZeroWithWarnings() {
        testYamlFail("---\n" +
                     "- CreateTable: t (vc varchar(32))\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES ('a')\n" +
                     "---\n" +
                     "- Statement: SELECT DATE(vc) FROM t\n" +
                     "- warnings_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountWithWarnings() {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings_count: 4\n");
    }

    @Test
    public void testStatementWarningsCountRegexpWithWarnings() {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings_count: !re '[1-9]'\n");
    }

    /* Test Statement warnings */

    @Test
    public void testStatementWarningsNoValue() {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings:\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsRepeated() {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings: [[1]]\n" +
                     "- warnings: [[2]]\n");
    }

    @Test
    public void testStatementWarningsNullValue() {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings: null\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsValueNotSequence() {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: 33");
    }

    @Test
    public void testStatementWarningsValueNotSequenceOfSequences() {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: [33]");
    }

    @Test
    public void testStatementWarningsValueNotSequenceOfSequencesOfScalars() {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: [[[33]]]");
    }

    @Test
    public void testStatementWarningsEmpty() {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: []");
    }

    @Test
    public void testStatementWarningsEmptyElement() {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: [[]]");
    }

    @Test
    public void testStatementWarningsSequenceTooLong() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- warnings: [[1, a, b]]");
    }

    @Test
    public void testStatementWarningsCodeNotScalar() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- warnings: [[[x, y]]]");
    }

    @Test
    public void testStatementWarningsWrongCode() {
        testYamlFail("---\n" +
                     "- CreateTable: t (vc varchar(32))\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES ('a')\n" +
                     "---\n" +
                     "- Statement: SELECT DATE(vc) FROM t\n" +
                     "- warnings: [[1234], [1234], [1234], [1234]]\n");
    }

    @Test
    public void testStatementWarningsWrongMessage() {
        testYamlFail("---\n" +
                     "- CreateTable: t (vc varchar(32))\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES ('a')\n" +
                     "---\n" +
                     "- Statement: SELECT DATE(vc) FROM t\n" +
                     "- warnings: [[22007, 'Foo'],\n" +
                     "    [22007, 'Foo'],\n" +
                     "    [22007, 'Foo'],\n" +
                     "    [22007, 'Foo']]");
    }

    @Test
    public void testStatementWarningsRightMessage() {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings: [[22007, 'Invalid date format: a'],\n" +
                 "    [22007, 'Invalid date format: a'],\n" +
                 "    [22007, 'Invalid timestamp format: a'],\n" +
                 "    [22007, 'Invalid year format: a']]");
    }

    @Test
    public void testStatementWarningsDontMatchCount() {
        testYamlFail("---\n" +
                     "- CreateTable: t (vc varchar(32))\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES ('a')\n" +
                     "---\n" +
                     "- Statement: SELECT DATE(vc) FROM t\n" +
                     "- warnings_count: 3\n" +
                     "- warnings: [[22007, 'Invalid date format: a'],\n" +
                     "    [22007, 'Invalid date format: a'],\n" +
                     "    [22007, 'Invalid timestamp format: a'],\n" +
                     "    [22007, 'Invalid year format: a']]");
    }

    @Test
    public void testStatementWarningsMatchCount() {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings_count: 4\n" +
                 "- warnings: [[22007, 'Invalid date format: a'],\n" +
                 "    [22007, 'Invalid date format: a'],\n" +
                 "    [22007, 'Invalid timestamp format: a'],\n" +
                 "    [22007, 'Invalid year format: a']]");
    }

    @Test
    public void testStatementWarningsRegexp() {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings: [[!re '[0-9]+', !re 'Invalid .*'],\n" +
                 "    [!re '[0-9]+', !re 'Invalid .*'],\n" +
                 "    [!re '[0-9]+', !re 'Invalid .*'],\n" +
                 "    [!re '[0-9]+', !re 'Invalid .*']]");
    }


    /* Other methods */

    private void testYaml(String yaml) {
	if (DEBUG) {
	    StackTraceElement[] callStack =
		Thread.currentThread().getStackTrace();
	    if (callStack.length > 2) {
		String testMethod = callStack[2].getMethodName();
		System.err.println(testMethod + ": ");
	    }
	}
	try {
	    new YamlTester(null, new StringReader(yaml), connection).test();
	} catch (RuntimeException e) {
	    if (DEBUG) {
		System.err.println("Test failed:");
		e.printStackTrace();
	    }
	    throw e;
	} catch (Error e) {
	    if (DEBUG) {
		System.err.println("Test failed:");
		e.printStackTrace();
	    }
	    throw e;
	}
	if (DEBUG) {
	    System.err.println("Test passed");
	}

    }

    private void testYamlFail(String yaml) {
	if (DEBUG) {
	    StackTraceElement[] callStack =
		Thread.currentThread().getStackTrace();
	    if (callStack.length > 2) {
		String testMethod = callStack[2].getMethodName();
		System.err.println(testMethod + ": ");
	    }
	}
	try {
	    new YamlTester(null, new StringReader(yaml), connection).test();
	    if (DEBUG) {
		System.err.println("Test failed: Expected exception");
	    }
	} catch (Throwable t) {
	    if (DEBUG) {
		System.err.println(t);
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
