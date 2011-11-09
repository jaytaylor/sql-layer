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
import java.io.StringReader;
import java.io.Writer;

import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Run basic tests of the {@code YamlTester} for YAML files that specify
 * passing tests.
 */
public class YamlTesterIT extends PostgresServerYamlITBase {

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

    /* Test Include */

    @Test
    public void testIncludeMissingValue() {
	testYamlFail("- Include:");
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
	File include = File.createTempFile("include", null);
	include.deleteOnExit();
	testYamlFail("- Include: somefile\n" +
		     "- foo: bar");
    }

    @Test
    public void testIncludeSimple() throws Exception {
	File include = File.createTempFile("include", null);
	include.deleteOnExit();
	writeFile(include,
		  "---\n" +
		  "- Statement: CREATE TABLE c (cid int)\n" +
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
		  "- Statement: CREATE TABLE c (cid int)\n");
	testYaml("---\n" +
		 "- Include: " + include1 + "\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM c\n" +
		 "- output: [[1], [2]]\n");
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
		  "- Statement: CREATE TABLE c (cid int)\n");
	testYaml("---\n" +
		 "- Include: " + include1 + "\n" +
		 "---\n" +
		 "- Statement: SELECT * FROM c\n" +
		 "- output: [[1], [2]]\n");
    }

    /* Test Properties */

    @Test
    public void testPropertiesNoValue() {
	testYamlFail("---\n" +
		     "- Properties:\n");
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

    /* Test Statement */

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

    /* Test Statement params */

    @Test
    public void testStatementParamsNoValue() {
	testYamlFail("- Statement: a b c\n" +
		     "- params:");
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
    public void testStatementParamsTypesNoValue() {
	testYamlFail("- Statement: a b c\n" +
		     "- params: [[a, b]]\n" +
		     "- param_types:");
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
	    "- Statement: CREATE TABLE c (cid int)\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1), (2)\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c where cid = ?\n" +
	    "- params: [[1]]\n" +
	    "- param_types: [DECIMAL]\n" +
	    "- output: [[1]]");
    }

    /* Test Statement output */

    @Test
    public void testStatementOutputNoValue() {
	testYamlFail("- Statement: a b c\n" +
		     "- output:");
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
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32));\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32));\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32));\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32));\n" +
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
    public void testStatementOutputDontCare() {
	testYaml(
	    "---\n" +
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32))\n" +
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
    public void testStatementOutputNullAndEmpty() {
	testYaml(
	    "---\n" +
	    "- Statement: CREATE TABLE customers (cid int, name varchar(32))\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32))\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE parts (part varchar(32), product varchar(32))\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE parts (part varchar(32), product varchar(32))\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE parts (part varchar(32), product varchar(32))\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE parts (part varchar(32), product varchar(32))\n" +
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
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- row_count:");
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
    public void testStatementRowCountWrongValue() {
	testYamlFail(
	    "---\n" +
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32));\n" +
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
	    "- Statement:\n" +
	    "    CREATE TABLE customers (cid int, name varchar(32));\n" +
	    "---\n" +
	    "- Statement:\n" +
	    "    INSERT INTO customers VALUES (1, 'Smith'), (2, 'Jones');\n" +
	    "- row_count: 2\n");
    }

    /* Test Statement output_types */

    @Test
    public void testStatementOutputTypesNoValue() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- output_types:");
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
	    "- Statement: CREATE TABLE c (cid int, name varchar(32))\n" +
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
	    "- Statement: CREATE TABLE c (cid int, name varchar(32))\n" +
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
	    "- Statement: CREATE TABLE c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- output_types: [INTEGER, VARCHAR]");
    }

    /* Test Statement explain */

    @Test
    public void testStatementExplainNoValue() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- explain:");
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
	    "- Statement: CREATE TABLE c (cid int, name varchar(32))\n" +
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
	    "- Statement: CREATE TABLE c (cid int, name varchar(32))\n" +
	    "---\n" +
	    "- Statement: INSERT INTO c VALUES (1, 'Smith'), (2, 'Jones')\n" +
	    "---\n" +
	    "- Statement: SELECT * FROM c\n" +
	    "- explain: |\n" +
	    "    project([Field(0), Field(1)])\n" +
	    "      Filter_Default([user.c])\n" +
	    "        GroupScan_Default(full scan on _akiban_c)\n");
    }

    /* Test Statement error */

    @Test
    public void testStatementErrorNoValue() {
	testYamlFail(
	    "---\n" +
	    "- Statement: a b c\n" +
	    "- error:");
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
	    "- Statement: CREATE TABLE a (i int)\n" +
	    "- error: [1]");
    }

    @Test
    public void testStatementErrorWrongError() {
	testYamlFail(
	    "---\n" +
	    "- Statement: CREATE TABLE a (i int\n" +
	    "- error: [37]");
    }

    @Test
    public void testStatementErrorRightError() {
	testYaml(
	    "---\n" +
	    "- Statement: CREATE TABLE a (i int\n" +
	    "- error: [42000]");
    }

    @Test
    public void testStatementErrorWrongErrorMessage() {
	testYamlFail(
	    "---\n" +
	    "- Statement: CREATE TABLE a (i int\n" +
	    "- error: [42000, nope]");
    }

    @Test
    public void testStatementErrorRightErrorMessage() {
	testYaml(
	    "---\n" +
	    "- Statement: CREATE TABLE a (i int\n" +
	    "- error:\n" +
	    "  - 42000\n" +
	    "  - |\n" +
	    "    ERROR: [] com.akiban.sql.parser.ParseException: " +
	    "Encountered \"<EOF>\" at line 1, column 21.\n" +
	    "    Was expecting one of:\n" +
	    "        \")\" ...\n" +
	    "        \",\" ...\n" +
	    "        : CREATE TABLE a (i int\n");
    }

    /* Other methods */

    private void testYaml(String yaml) {
	new YamlTester(null, new StringReader(yaml), connection).test();
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
	    testYaml(yaml);
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
