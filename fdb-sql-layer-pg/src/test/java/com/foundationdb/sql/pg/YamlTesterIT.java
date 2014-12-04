/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.pg;

import com.foundationdb.sql.test.YamlTester;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the {@code YamlTester} class. */
public class YamlTesterIT extends PostgresServerITBase {
    private static final Logger LOG = LoggerFactory.getLogger(YamlTesterIT.class);

    static
    {
        //ConverterTestUtils.setGlobalTimezone("UTC");
    }
    /* Tests */

    /* Test general syntax */

    @Test
    public void testIncorrectYaml() throws Exception {
	testYamlFail("!!blah-blah");
    }

    @Test
    public void testEmptyYaml() throws Exception {
	testYamlFail("");
    }

    @Test
    public void testEmptyDocument() throws Exception {
	testYamlFail("---\n...");
    }

    @Test
    public void testNotSequence() throws Exception {
	testYamlFail("a: b\n");
    }

    @Test
    public void testFirstElementNotMap() throws Exception {
	testYamlFail("- a\n");
    }

    @Test
    public void testFirstElementKeyNumber() throws Exception {
	testYamlFail("- 1\n");
    }

    @Test
    public void testFirstElementKeySequence() throws Exception {
	testYamlFail("- [a]\n");
    }

    @Test
    public void testFirstElementKeyNotKnown() throws Exception {
	testYamlFail("- UnknownCommand:");
    }

    /* Test !select-engine general syntax */

    @Test
    public void testSelectEngineMissingValue() throws Exception {
	testYamlFail("- Statement: !select-engine");
    }

    @Test
    public void testSelectEngineScalarValue() throws Exception {
	testYamlFail("- Statement: !select-engine foo");
    }

    @Test
    public void testSelectEngineSequenceValue() throws Exception {
	testYamlFail("- Statement: !select-engine [foo, bar]");
    }

    @Test
    public void testSelectEngineKeyNotScalar() throws Exception {
	testYamlFail("- Statement: !select-engine { [foo, bar]: baz }");
    }

    @Test
    public void testSelectEngineMatch() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: !select-engine\n" +
		 "    { foo: bar,\n" +
		 "      it: INSERT INTO t VALUES (1) }\n" +
		 "...");
    }

    @Test
    public void testSelectEngineNoMatch() throws Exception {
	testYamlFail("- Statement: !select-engine { foo: bar }"); // Empty statement
    }

    @Test
    public void testSelectEngineKeyItOverAllPrecedence() throws Exception {
	testYaml("---\n" +
                 "- CreateTable: t (bigint_field bigint)\n" +
                 "---\n" +
                 "- Statement: !select-engine\n" +
                 "    { it: SELECT * FROM t, all: oops }");
    }

    @Test
    public void testSelectEngineKeyItOverAllPrecedenceReversed() throws Exception {
	testYaml("---\n" +
                 "- CreateTable: t (bigint_field bigint)\n" +
                 "---\n" +
                 "- Statement: !select-engine\n" +
                 "    { all: oops, it: SELECT * FROM t }");
    }

    @Test
    public void testSelectEngineKeyItOverAllPrecedenceNull() throws Exception {
	testYaml("---\n" +
                 "- Statement: !select-engine { it: select null, all: oops }");
	testYaml("---\n" +
                 "- Statement: !select-engine { all: oops, it: select null }");
	testYamlFail("---\n" +
                     "- Statement: !select-engine { it: oops, all: null }");
	testYamlFail("---\n" +
                     "- Statement: !select-engine { all: null, it: oops }");
    }

    @Test
    public void testSelectEngineKeyAll() throws Exception {
	testYaml("---\n" +
                 "- CreateTable: t (bigint_field bigint)\n" +
                 "---\n" +
                 "- Statement: !select-engine\n" +
                 "    { all: SELECT * FROM t, foo: oops }");
    }

    @Test
    public void testSelectEngineKeyAllNull() throws Exception {
	testYaml("---\n" +
                 "- Statement: !select-engine { all: select null, foo: oops }");
	testYaml("---\n" +
                 "- Statement: !select-engine { foo: oops, all: select null }");
    }

    /* Test Include */

    @Test
    public void testIncludeMissingValue() throws Exception {
	testYaml("- Include:");
    }

    @Test
    public void testIncludeNullValue() throws Exception {
	testYaml("- Include: null");
    }

    @Test
    public void testIncludeWrongTypeValue() throws Exception {
	testYamlFail("- Include: [a, b]");
    }

    @Test
    public void testIncludeFileNotFound() throws Exception {
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
		 "- Include: " + include.toURI().toURL() + "\n" +
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
		  "- Include: " + include2.toURI().toURL() + "\n" +
		  "---\n" +
		  "- Statement: INSERT INTO c VALUES (1), (2)\n");
	writeFile(include2,
		  "---\n" +
		  "- CreateTable: c (cid int)\n");
	testYaml("---\n" +
		 "- Include: " + include1.toURI().toURL() + "\n" +
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
		 "- Include: " + include1.toURI().toURL() + "\n" +
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
        String include = includeFile.toURI().toURL().toString().replaceAll("\\\\", "\\\\");
	testYaml("---\n" +
		 "- Include: !select-engine { foo: bar, it: '" + include +
		 "' }\n" +
		 "---\n" +
		 "- Statement: SELECT bigint_field FROM t\n" +
		 "...");
    }

    /* Test Properties */

    @Test
    public void testPropertiesNoValue() throws Exception {
	testYamlFail("---\n" +
		     "- Properties:\n");
    }

    @Test
    public void testPropertiesNullValue() throws Exception {
	testYamlFail("---\n" +
		     "- Properties: null\n");
    }

    @Test
    public void testPropertiesValueNotString() throws Exception {
	testYamlFail("---\n" +
		     "- Properties: 33\n");
    }

    @Test
    public void testPropertiesValueUnknownEngine() throws Exception {
	testYaml("---\n" +
		 "- Properties: gorp\n");
    }

    @Test
    public void testPropertiesValueRegexp() throws Exception {
        testYamlFail("---\n" +
                     "- Properties: !re all\n");
    }

    @Test
    public void testPropertiesValueAttributesNotSequenceOfMaps() throws Exception {
	testYamlFail("---\n" +
		     "- Properties: all\n" +
		     "33\n");
    }

    @Test
    public void testPropertiesValueSuppressedNoValue() throws Exception {
	testYamlFail("---\n" +
		     "- Properties: all\n" +
		     "- suppressed:\n");
    }

    @Test
    public void testPropertiesValueSuppressedBadValue() throws Exception {
	testYamlFail("---\n" +
		     "- Properties: all\n" +
		     "- suppressed: gorp\n");
    }

    @Test
    public void testPropertiesValueSuppressedAll() throws Exception {
	testYaml("---\n" +
		 "- Properties: all\n" +
		 "- suppressed: true\n" +
		 "---\n" +
		 "- Never seen!\n");
    }

    @Test
    public void testPropertiesValueSuppressedIt() throws Exception {
	testYaml("---\n" +
		 "- Properties: it\n" +
		 "- suppressed: true\n" +
		 "---\n" +
		 "- Never seen!\n");
    }

    /* Test CreateTable */

    @Test
    public void testCreateTableNoValue() throws Exception {
	testYamlFail("- CreateTable:");
    }

    @Test
    public void testCreateTableNullValue() throws Exception {
	testYamlFail("- CreateTable: null");
    }

    @Test
    public void testCreateTableSequenceValue() throws Exception {
	testYamlFail("- CreateTable: [a, b, c]");
    }

    @Test
    public void testCreateTableUnexpectedAttribute() throws Exception {
	testYamlFail("- CreateTable: foo (int_field int)\n" +
		     "- unexpected_attribute: quux");
    }

    @Test
    public void testCreateTableSuccess() throws Exception {
	testYaml("- CreateTable: foo (int_field int)");
    }
    
    @Test
    public void testCreateTableSelectEngine1() throws Exception {
        testYaml("- CreateTable: !select-engine { all: 't (int_field int)', it: 't (int_field int)' } ");
        testYaml("- CreateTable: !select-engine {it: 't2 (int_field int)', fts: 't2 (int_field int)', all: 't2 (int_field int)' } ");
    }

    /* Test CreateTable error */

    @Test
    public void testCreateTableErrorNoValue() throws Exception {
	testYamlFail(
	    "- CreateTable: a b c\n" +
	    "- error:");
    }

    @Test
    public void testCreateTableErrorNotSequence() throws Exception {
	testYamlFail(
	    "- CreateTable: a b c\n" +
	    "- error: fooey");
    }

    @Test
    public void testCreateTableErrorEmptySequence() throws Exception {
	testYamlFail(
	    "- CreateTable: a b c\n" +
	    "- error: []");
    }

    @Test
    public void testCreateTableErrorRepeated() throws Exception {
        testYamlFail("- CreateTable: a (i int\n" +
                     "- error: [42000]\n" +
                     "- error: [42000]\n");
    }

    @Test
    public void testCreateTableErrorSequenceTooLong() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a b c\n" +
	    "- error: [1, a, b]");
    }

    @Test
    public void testCreateTableErrorCodeNotScalar() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a b c\n" +
	    "- error: [[x, y], a, b]");
    }

    @Test
    public void testCreateTableErrorNotError() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a (i int)\n" +
	    "- error: [1]");
    }

    @Test
    public void testCreateTableNotErrorError() throws Exception {
	testYamlFail("- CreateTable: a (int_field int");
    }

    @Test
    public void testCreateTableErrorWrongError() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: [37]");
    }

    @Test
    public void testCreateTableErrorRightError() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: [42000]");
    }

    @Test
    public void testCreateTableErrorWrongErrorMessage() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: [42000, nope]");
    }

    @Test
    public void testCreateTableErrorRightErrorMessage() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error:\n" +
	    "  - 42000\n" +
	    "  - |\n" +
	    "    ERROR: Encountered \"<EOF>\" at line 1, column 22.\n" +
	    "    Was expecting one of:\n" +
	    "        \")\" ...\n" +
	    "        \",\" ...\n" +
	    "        \n" +
	    "      Position: 22");
    }

    @Test
    public void testCreateTableErrorSelectEngine() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: a (i int\n" +
	    "- error: !select-engine { it: [42000], fdb-sql: [33] }");
    }

    /* Test Statement */

    @Test
    public void testStatementNoValue() throws Exception {
	testYamlFail("- Statement:"); // Empty statement
    }

    @Test
    public void testStatementNullValue() throws Exception {
	testYamlFail("- Statement: null"); // Empty statement
    }

    @Test
    public void testStatementValueInteger() throws Exception {
	testYamlFail("- Statement: 3");
    }

    @Test
    public void testStatementValueSequence() throws Exception {
	testYamlFail("- Statement: [a, b, c]");
    }

    @Test
    public void testStatementUnknownAttribute() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- unknown_attrib: x y z");
    }

    @Test
    public void testStatementCreateTable() throws Exception {
	testYaml("- Statement: create TABLE foo (int_field int)");
    }

    @Test
    public void testStatementSelectEngineMatch() throws Exception {
	testYaml("- Statement: !select-engine { it: MORP }\n" +
		 "- error: [42000]");
    }

    @Test
    public void testStatementSelectEngineMatchAll() throws Exception {
	testYaml("- Statement: !select-engine { all: MORP }\n" +
		 "- error: [42000]");
    }

    @Test
    public void testStatementSelectEngineNoMatch() throws Exception {
	testYamlFail("- Statement: !select-engine { sys: MORP }"); // Empty statement
    }

    /* Test Statement params */

    @Test
    public void testStatementParamsNoValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- params:");
    }

    @Test
    public void testStatementParamsNullValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- params: null");
    }

    @Test
    public void testStatementParamsRepeated() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (i int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t WHERE i = ?\n" +
                     "- params: [[1]]\n" +
                     "- params: [[2]]\n");
    }

    @Test
    public void testStatementParamsNotSequence() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- params: 3");
    }

    @Test
    public void testStatementParamsValueSequenceOfMaps() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [a: b]");
    }

    @Test
    public void testStatementParamsValueEmpty() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- params: []");
    }

    @Test
    public void testStatementParamsValueEmptySequence() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[], []]");
    }

    @Test
    public void testStatementParamsValueDifferentLengthParams() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[a, b], [c, d, e]]");
    }

    /* Test Statement param_types */

    @Test
    public void testStatementParamsTypesNoParams() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- param_types: [CHAR]");
    }

    @Test
    public void testStatementParamsTypesNoParamsNoValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- param_types:");
    }

    @Test
    public void testStatementParamsTypesNullParamsNullValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- params: null\n" +
		 "- param_types: null");
    }
 
    @Test
    public void testStatementParamsTypesNoValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t WHERE bigint_field = ?\n" +
		 "- params: [[1]]\n" +
		 "- param_types:");
    }

    @Test
    public void testStatementParamsTypesNullValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (bigint_field bigint)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t WHERE bigint_field = ?\n" +
		 "- params: [[1]]\n" +
		 "- param_types: null");
    }

    @Test
    public void testStatementParamsTypesUnknown() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[a, b]]\n" +
		     "- param_types: [CHAR, WHATTYPE]");
    }

    @Test
    public void testStatementParamsTypesEmpty() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- param_types: []");
    }

    @Test
    public void testStatementParamsTypesRepeated() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (i int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t WHERE i = ?\n" +
                     "- params: [[1]]\n" +
                     "- param_types: [INTEGER]\n" +
                     "- param_types: [INTEGER]\n");
    }

    @Test
    public void testStatementParamsTypesTooMany() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- param_types: [CHAR, INTEGER, BOOLEAN]\n" +
		     "- params: [[a, b]]");
    }

    @Test
    public void testStatementParamsTypesTooFew() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[a, b]]\n" +
		     "- param_types: [CHAR]");
    }

    @Test
    public void testStatementParamTypesSuccess() throws Exception {
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
    public void testStatementParamTypesSelectEngine() throws Exception {
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
    public void testStatementOutputNoValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (id int)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- output:");
    }

    @Test
    public void testStatementOutputRepeated() throws Exception {
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
    public void testStatementOutputNullValue() throws Exception {
	testYaml("---\n" +
		 "- CreateTable: t (id int)\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM t\n" +
		 "- output: null");
    }

    @Test
    public void testStatementOutputValueNotSequence() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- output: 33");
    }

    @Test
    public void testStatementOutputValueNotSequenceOfSequences() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [33]");
    }

    @Test
    public void testStatementOutputValueNotSequenceOfSequencesOfScalars() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [[[33]]]");
    }

    @Test
    public void testStatementOutputEmpty() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- output: []");
    }

    @Test
    public void testStatementOutputEmptyRow() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [[], []]");
    }

    @Test
    public void testStatementOutputDifferentRowLengths() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- output: [[a], [b], [c, d, e]]");
    }

    @Test
    public void testStatementOutputWrongResults() throws Exception {
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
    public void testStatementOutputMissingRow() throws Exception {
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
    public void testStatementOutputExtraRow() throws Exception {
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
    public void testStatementOutputRightResults() throws Exception {
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
    public void testStatementOutputEmptyValue() throws Exception {
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
    public void testStatementOutputDontCare() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output_already_ordered:\n" +
	    "  - [!dc dc, Smith]\n" +
	    "  - [2, !dc dc]");
    }

    @Test
    public void testStatementOutputDontCareMissingValue() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FROM customers\n" +
	    "- output:\n" +
	    "  - [!dc]\n");
    }

    @Test
    public void testStatementOutputRegexp() throws Exception {
    	testYaml(
    	    "---\n" +
    	    "- CreateTable: t (name varchar(32))\n" +
    	    "---\n" +
    	    "- Statement: INSERT INTO t VALUES\n" +
	    "    ('hubba-hubba'), ('abc123')\n" +
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output_already_ordered:\n" +
    	    "  - [!re '([^-]*)-{1}']\n" +
    	    "  - [!re '[a-z]+[0-9]+']\n");
    }

    @Test
    public void testStatementOutputRegexpMissingValue() throws Exception {
    	testYamlFail(
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output:\n" +
    	    "  - [!re]\n");
    }

    @Test
    public void testStatementOutputRegexpNonScalarValue() throws Exception {
    	testYamlFail(
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output:\n" +
    	    "  - [!re [a, b, c]]\n");
    }

    @Test
    public void testStatementOutputRegexpSelectEngine() throws Exception {
    	testYaml(
    	    "---\n" +
    	    "- CreateTable: t (name varchar(32))\n" +
    	    "---\n" +
    	    "- Statement: INSERT INTO t VALUES\n" +
	    "    ('abc'), ('123')\n" +
    	    "---\n" +
    	    "- Statement: SELECT * FROM t\n" +
    	    "- output_already_ordered:\n" +
	    "    !select-engine\n" +
	    "      it: [[!re '[a-z]+'], [!re '[0-9]+']]\n" +
	    "      foo: bar\n");
    }

    @Test
    public void testStatementOutputNullAndEmpty() throws Exception {
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
    public void testStatementOutputParamsRightResults() throws Exception {
        testYaml(
            "---\n" +
            "- CreateTable: customers (cid int, name varchar(32))\n" +
            "---\n" +
            "- Statement:\n" +
            "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n");
        testYaml(
            "---\n" +
            "- Statement: SELECT * FROM customers WHERE name = ?\n" +
            "- params: [[Jones]]\n" +
            "- output:\n" +
            "  - [2, Jones]");
        // Too many params
        testYamlFail(
            "---\n" +
                "- Statement: SELECT * FROM customers WHERE name = ?\n" +
                "- params: [[Jones], [Smith], [Smith]]\n");
    }

    @Test
    public void testStatementOutputParamsExtraResultsOneCall() throws Exception {
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
    public void testStatementOutputParamsExtraResultsExtraCall() throws Exception {
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
    public void testStatementOutputParamsExtraOutputOneCall() throws Exception {
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
    public void testStatementOutputParamsWrongOutput() throws Exception {
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
    public void testStatementRowCountNoValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- row_count:");
    }

    @Test
    public void testStatementRowCountNullValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- row_count: null");
    }

    @Test
    public void testStatementRowCountNotInteger() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- row_count: hello");
    }

    @Test
    public void testStatementRowCountNegative() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- row_count: -33");
    }

    @Test
    public void testStatementRowCountRepeated() throws Exception {
        testYamlFail("---\n" +
                     "- Statement: a b c\n" +
                     "- row_count: 1\n" +
                     "- row_count: 1\n");
    }

    @Test
    public void testStatementRowCountWrongValue() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "- row_count: 1\n");
    }

    @Test
    public void testStatementRowCountDifferentFromOutput() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output: [[a]]\n" +
	    "- row_count: 2");
    }

    @Test
    public void testStatementRowCountCorrect() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "- row_count: 2\n");
    }

    @Test
    public void testStatementRowCountSelectEngine() throws Exception {
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
    public void testStatementOutputTypesNoValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- output_types:");
    }

    @Test
    public void testStatementOutputTypesRepeated() throws Exception {
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
    public void testStatementOutputTypesNullValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- output_types: null");
    }

    @Test
    public void testStatementOutputTypesNotSequence() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: 33");
    }

    @Test
    public void testStatementOutputTypesEmpty() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: []");
    }

    @Test
    public void testStatementOutputTypesWrongLengthFromOutput() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: [BOOLEAN]\n" +
	    "- output: [[a, b, c]]");
    }

    @Test
    public void testStatementOutputTypesUnknownType() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types: [GROOVY]");
    }

    @Test
    public void testStatementOutputTypesWrongNumberOfTypes() throws Exception {
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
    public void testStatementOutputTypesWrongType() throws Exception {
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
    public void testStatementOutputTypesCorrectTypes() throws Exception {
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
    public void testStatementOutputTypesSelectEngine() throws Exception {
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
    public void testStatementExplainNoValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- explain:");
    }

    @Test
    public void testStatementExplainRepeated() throws Exception {
	testYamlFail("---\n" +
                     "- CreateTable: t (i int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- explain: foo\n" +
                     "- explain: bar\n");
    }

    @Test
    public void testStatementExplainNullValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- explain: null");
    }

    @Test
    public void testStatementExplainSequenceValue() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- explain: [d, e, f]");
    }

    @Test
    public void testStatementExplainWrong() throws Exception {
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
    public void testStatementExplainRight() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- explain: |\n" +
	    "    Project_Default(c.cid, c.name)\n" +
	    "      Filter_Default(c)\n" +
	    "        GroupScan_Default(c)");
    }

    @Test
    public void testStatementExplainSelectEngine() throws Exception {
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
    public void testStatementErrorNoValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- error:");
    }

    @Test
    public void testStatementErrorRepeated() throws Exception {
	testYamlFail("---\n" +
                     "- CreateTable: t (int_field int)\n" +
                     "---\n" +
                     "- Statement: WHAT IS THIS\n" +
                     "- error: [42000]\n" +
                     "- error: [42000]\n");
    }

    @Test
    public void testStatementErrorAndOutput() throws Exception {
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
    public void testStatementErrorAndRowCount() throws Exception {
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
    public void testStatementErrorNullValue() throws Exception {
	testYaml(
	    "---\n" +
	    "- CreateTable: t (int_field int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM t\n" +
	    "- error: null");
    }

    @Test
    public void testStatementErrorNotSequence() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: fooey");
    }

    @Test
    public void testStatementErrorEmptySequence() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: []");
    }

    @Test
    public void testStatementErrorSequenceTooLong() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: [1, a, b]");
    }

    @Test
    public void testStatementErrorCodeNotScalar() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error: [[x, y], a, b]");
    }

    @Test
    public void testStatementErrorNotError() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- CreateTable a (i int)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM a\n" +
	    "- error: [1]");
    }

    @Test
    public void testStatementErrorWrongError() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [37]");
    }

    @Test
    public void testStatementErrorRightError() throws Exception {
	testYaml(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [42000]");
    }

    @Test
    public void testStatementErrorWrongErrorMessage() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [42000, nope]");
    }

    @Test
    public void testStatementErrorRightErrorMessage() throws Exception {
	testYaml(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error:\n" +
	    "  - 42000");
    }

    @Test
    public void testStatementErrorSelectEngineNoMatch() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: !select-engine { foo: [bar] }");
    }

    @Test
    public void testStatementErrorSelectEngineNoMatchWithMessage() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [!select-engine { foo: bar }, Hello]");
    }

    @Test
    public void testStatementErrorSelectEngineMatch() throws Exception {
	testYaml(
	    "---\n" +
	    "- Statement: SELECT * FR\n" +
	    "- error: [!select-engine { it: 42000, all: 42 }]");
    }

    /* Test Statement warnings_count */
    @Test
    public void testStatementWarningsCountNoValue() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings_count:\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountRepeated() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 1\n" +
                     "- warnings_count: 2\n");
    }

    @Test
    public void testStatementWarningsCountNullValue() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings_count: null\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountString() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 'abc'\n" +
                     "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountWrongValue() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 1\n" +
                     "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountDifferentFromWarnings() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 2\n" +
                     "- warnings: [[ 'abc' ]]\n" +
                     "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountNonZeroNoWarnings() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings_count: 1\n");
    }

    @Test
    public void testStatementWarningsCountZero() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountZeroWithWarnings() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (vc varchar(32))\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES ('a')\n" +
                     "---\n" +
                     "- Statement: SELECT DATE(vc) FROM t\n" +
                     "- warnings_count: 0\n");
    }

    @Test
    public void testStatementWarningsCountWithWarnings() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings_count: 1\n");
    }

    @Test
    public void testStatementWarningsCountRegexpWithWarnings() throws Exception {
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
    public void testStatementWarningsNoValue() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings:\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsRepeated() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (f int)\n" +
                     "---\n" +
                     "- Statement: SELECT * FROM t\n" +
                     "- warnings: [[1]]\n" +
                     "- warnings: [[2]]\n");
    }

    @Test
    public void testStatementWarningsNullValue() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (f int)\n" +
                 "---\n" +
                 "- Statement: SELECT * FROM t\n" +
                 "- warnings: null\n" +
                 "- row_count: 0\n");
    }

    @Test
    public void testStatementWarningsValueNotSequence() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: 33");
    }

    @Test
    public void testStatementWarningsValueNotSequenceOfSequences() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: [33]");
    }

    @Test
    public void testStatementWarningsValueNotSequenceOfSequencesOfScalars() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: [[[33]]]");
    }

    @Test
    public void testStatementWarningsEmpty() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: []");
    }

    @Test
    public void testStatementWarningsEmptyElement() throws Exception {
	testYamlFail("- Statement: a b c\n" +
		     "- warnings: [[]]");
    }

    @Test
    public void testStatementWarningsSequenceTooLong() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- warnings: [[1, a, b]]");
    }

    @Test
    public void testStatementWarningsCodeNotScalar() throws Exception {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- warnings: [[[x, y]]]");
    }

    @Test
    public void testStatementWarningsWrongCode() throws Exception {
        testYamlFail("---\n" +
                     "- CreateTable: t (vc varchar(32))\n" +
                     "---\n" +
                     "- Statement: INSERT INTO t VALUES ('a')\n" +
                     "---\n" +
                     "- Statement: SELECT DATE(vc) FROM t\n" +
                     "- warnings: [[1234], [1234], [1234], [1234]]\n");
    }

    @Test
    public void testStatementWarningsWrongMessage() throws Exception {
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
    public void testStatementWarningsRightMessage() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings: [[22007, \"Invalid date format: a\"]]");
    }

    
    @Test
    public void testStatementWarningsDontMatchCount() throws Exception {
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
    public void testStatementWarningsMatchCount() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings_count: 1\n" +
                "- warnings: [[22007, \"Invalid date format: a\"]]");
    }

    @Test
    public void testStatementWarningsRegexp() throws Exception {
        testYaml("---\n" +
                 "- CreateTable: t (vc varchar(32))\n" +
                 "---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n" +
                 "---\n" +
                 "- Statement: SELECT DATE(vc) FROM t\n" +
                 "- warnings: [[!re '[0-9]+', !re \".*\"]]");
    }

    @Test
    public void testConnectionReuse() throws Exception {
        boolean failed = false;
        try {
            testYaml("---\n" +
                     "- CreateTable: t (vc varchar(32))\n" +
                     "---\n" +
                     "- Statement: SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY\n" +
                     "---\n" +
                     "- Statement: SELECT garbage FROM t\n");
        }
        catch (AssertionError e) {
            failed = true;
        }
        assertTrue("First half failed", failed);
        // ^ Left connection read-only.
        testYaml("---\n" +
                 "- Statement: INSERT INTO t VALUES ('a')\n");
    }

    @Test
    public void testNegativeColumnCount_too_small() throws Exception {
        testYamlFail("---\n" +
                    "- CreateTable: testA (c1 int)\n"+
                    "---\n"+
                    "- Statement: INSERT INTO testA values (1)\n"+
                    "---\n"+
            "- Statement: SELECT 1,5,7,8,9,1,2,6 FROM testA\n"+
                    "- output: [[1,5,7,8,9,1,2]]");
    }
    
    @Test
    public void testNegativeColumnCount_too_many() throws Exception {
        testYamlFail("---\n" +
                    "- CreateTable: testA (c1 int)\n"+
                    "---\n"+
                    "- Statement: INSERT INTO testA values (1)\n"+
                    "---\n"+
            "- Statement: SELECT 1,5,7,8,9,1,2,6 FROM testA\n"+
                    "- output: [[1,5,7,8,9,1,2,6,99]]");
    }
    
    @Test
    public void testRowCount_post() throws Exception {
        testYaml("---\n" +
                    "- CreateTable: testA (c1 int)\n"+
                    "---\n"+
                    "- Statement: INSERT INTO testA values (1)\n"+
                    "---\n"+
                    "- Statement: SELECT 1,5,7,8,9,1,2,6 FROM testA\n"+
                    "- output: [[1,5,7,8,9,1,2,6]]\n"+
                    "- row_count: 1");
    } 
    
    @Test
    public void testRowCount_pre() throws Exception {
        testYaml("---\n" +
                "- CreateTable: testA (c1 int)\n"+
                "---\n"+
                "- Statement: INSERT INTO testA values (1)\n"+
                "---\n"+
        "- Statement: SELECT 1,5,7,8,9,1,2,6 FROM testA\n"+
                "- row_count: 1\n"+
                "- output: [[1,5,7,8,9,1,2,6]]\n");
    }
     
    @Test
    public void testRowCount_negative_pre() throws Exception {
        testYamlFail("---\n" +
                "- CreateTable: testA (c1 int)\n"+
                "---\n"+
                "- Statement: INSERT INTO testA values (1)\n"+
                "---\n"+
        "- Statement: SELECT 1,5,7,8,9,1,2,6 FROM testA\n"+
                "- row_count: 8\n"+
                "- output: [[1,5,7,8,9,1,2,6]]\n");
    }
    
    @Test
    public void testRowCount() throws Exception {
        testYamlFail("---\n" +
                "- CreateTable: testA (c1 int)\n"+
                "---\n"+
                "- Statement: INSERT INTO testA values (1)\n"+
                "---\n"+
                "- Statement: SELECT 1,5,7,8,9,1,2,6 FROM testA\n"+
                "- row_count: 8\n");
        
        testYamlFail("---\n" +
                "- CreateTable: testA (c1 int)\n"+
                "---\n"+
                "- Statement: INSERT INTO testA values (1)\n"+
                "---\n"+
        "- Statement: SELECT 1,5,7,8,9,1,2,6 FROM testA\n"+
                "- output: [[1,5,7,8,9,1,2,6]]\n" +
                "- row_count: 8\n");
    }
    
    @Test
    public void testOutput() throws Exception {
        testYaml("---\n" +
            "- CreateTable: test2 (c1 int, c2 varchar(25))\n" +
            "---\n" +
            "- Statement: INSERT INTO test2 values (1, 'A')\n" +
            "---\n" +
            "- Statement: INSERT INTO test2 values (-4, 'abc')\n" +
            "---\n" +
            "- Statement: INSERT INTO test2 values (234, 'Z')\n"
        );
        // Expected matches output order
        testYaml("---\n" +
                     "- Statement: SELECT * from test2 order by c1\n" +
                     "- output: [[-4, 'abc'],[1, 'A'],[234,'Z']]");
        // Expected does not match actual order (OK)
        testYaml("---\n" +
                 "- Statement: SELECT * from test2 order by c1 desc\n" +
                 "- output: [[-4, 'abc'],[1, 'A'],[234,'Z']]");
        // Too many columns
        testYamlFail("---\n" +
                 "- Statement: SELECT * from test2 order by c1\n" +
                 "- output: [[-4, 'abc', 'fake'],[1, 'A'],[234,'Z']]");
        // Too few columns
        testYamlFail("---\n" +
                 "- Statement: SELECT * from test2 order by c1\n" +
                 "- output: [[-4],[1, 'A'],[234,'Z']]");
        // Too many rows
        testYamlFail("---\n" +
                 "- Statement: SELECT * from test2 order by c1\n" +
                 "- output: [[-4, 'abc'],[1, 'A'],[234,'Z'],[999,'abba']]");
        // Too few rows
        testYamlFail("---\n" +
                "- Statement: SELECT * from test2 order by c1\n" +
                "- output: [[-4, 'abc'],[1, 'A']]");
    }
    
    @Test
    public void testOutputAlreadyOrdered() throws Exception {
        testYaml("---\n" +
                     "- CreateTable: test2 (c1 int, c2 varchar(25))\n" +
                     "---\n" +
                     "- Statement: INSERT INTO test2 values (1, 'A')\n" +
                     "---\n" +
                     "- Statement: INSERT INTO test2 values (-4, 'abc')\n" +
                     "---\n" +
                     "- Statement: INSERT INTO test2 values (234, 'Z')\n"
        );
        // Expected matches output order
        testYaml("---\n" +
                 "- Statement: SELECT * from test2 order by c1\n" +
                 "- output: [[-4, 'abc'],[1, 'A'],[234,'Z']]");
        // Expected does not match actual order
        testYamlFail("---\n" +
                     "- Statement: SELECT * from test2 order by c1 desc\n" +
                     "- output_already_ordered: [[-4, 'abc'],[1, 'A'],[234,'Z']]");
        // Too many columns
        testYamlFail("---\n" +
                     "- Statement: SELECT * from test2 order by c1\n" +
                     "- output_already_ordered: [[-4, 'abc', 'fake'],[1, 'A'],[234,'Z']]");
        // Too few columns
        testYamlFail("---\n" +
                     "- Statement: SELECT * from test2 order by c1\n" +
                     "- output_already_ordered: [[-4],[1, 'A'],[234,'Z']]");
        // Too many rows
        testYamlFail("---\n" +
                          "- Statement: SELECT * from test2 order by c1\n" +
                     "- output_already_ordered: [[-4, 'abc'],[1, 'A'],[234,'Z'],[999,'abba']]");
        // Too few rows
        testYamlFail("---\n" +
                     "- Statement: SELECT * from test2 order by c1\n" +
                     "- output_already_ordered: [[-4, 'abc'],[1, 'A']]");
    }
    
    
    @Test
    public void testJMXOutputWrongSize() throws Exception {
        testYamlFail ("---\n" +
                "- JMX: com.foundationdb:type=PostgresServer\n" + 
                "- get: StatementCacheCapacity\n" + 
                "- output: [['100', '200']]");
        testYamlFail ("---\n" +
                "- JMX: com.foundationdb:type=PostgresServer\n" + 
                "- get: StatementCacheCapacity\n" + 
                "- output: [['100'], ['200']]");
    }
    
    @Test
    public void testJMXSplitWrongSize() throws Exception {
        testYamlFail ("---\n" +
                "- JMX: com.foundationdb:type=PostgresServer\n" + 
                "- get: StatementCacheCapacity\n" + 
                "- split_result: [['100', '200']]");
    }
    
    @Test
    public void testJMXoutputAndSplit() throws Exception {
        testYamlFail ("---\n" +
                "- JMX: com.foundationdb:type=PostgresServer\n" + 
                "- get: StatementCacheCapacity\n" + 
                "- split_result: ['100']]\n" + 
                "- output: [['100']]");
        testYamlFail ("---\n" +
                "- JMX: com.foundationdb:type=PostgresServer\n" + 
                "- get: StatementCacheCapacity\n" + 
                "- output: [['100']]\n" +
                "- split_result: ['100']]"); 
        
    }
    
    @Test 
    public void testJMXOutput() throws Exception {
        testYaml ("---\n" +
        "- JMX: com.foundationdb:type=PostgresServer\n" + 
        "- get: StatementCacheCapacity\n" + 
        "- output: [['0']]");
    }
    
    @Test 
    public void testJMXSplit() throws Exception {
        testYaml("---\n" +
        "- JMX: com.foundationdb:type=PostgresServer\n" + 
        "- get: StatementCacheCapacity\n" + 
        "- split_result: [['0']]");
    }

    @Test
    public void testRetryCountDefault() throws Exception {
        testYaml("---\n" +
         "- Statement: SELECT 1\n" +
         "- retry_count: ");
    }

    @Test
    public void testRetryCountExplicit() throws Exception {
        testYaml("---\n" +
         "- Statement: SELECT 1\n" +
        "- retry_count: 5");
    }

    @Test
    public void testRetryCountNonInteger() throws Exception {
        testYamlFail("---\n" +
         "- Statement: SELECT * FROM t\n" +
         "- retry_count: [5]");
    }

    @Test
    public void testUseContext() throws Exception {
        testYaml ("---\n" +
           "- UseContext: default\n" +
           "- fixed: true"
                );
    }
    /* Other methods */

    private void testYaml(String yaml) throws Exception {
	if (LOG.isDebugEnabled()) {
	    StackTraceElement[] callStack =
		Thread.currentThread().getStackTrace();
	    if (callStack.length > 2) {
		    String testMethod = callStack[2].getMethodName();
            LOG.debug("{}: ", testMethod);
	    }
	}
	try {
	    new YamlTester(new StringReader(yaml), getConnection()).test();
	} catch (Exception e) {
        LOG.debug("Test failed:", e);
        forgetConnection();
	    throw e;
	} catch (Error e) {
        LOG.debug("Test failed", e);
        forgetConnection();
	    throw e;
	}
    LOG.debug("Test passed");
    }

    private void testYamlFail(String yaml) throws Exception {
    if(LOG.isDebugEnabled()) {
	    StackTraceElement[] callStack = Thread.currentThread().getStackTrace();
	    if (callStack.length > 2) {
		    String testMethod = callStack[2].getMethodName();
            LOG.debug("{}: ", testMethod);
	    }
	}
        try {
	    new YamlTester(new StringReader(yaml), getConnection()).test();
        LOG.debug("Test failed: Expected exception");
	} catch (Throwable t) {
        LOG.debug("Caught", t);
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
