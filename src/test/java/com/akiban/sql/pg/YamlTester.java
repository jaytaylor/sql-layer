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
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;

/**
 * A utility for testing SQL access based on the contents of a YAML file.
 */
/* General syntax:

   - One or more YAML documents.
   - Each document is a sequence whose first element is a map.
   - Key of first element's map is a command: a string with an uppercase first
     character

   Commands:

   Include
   - Syntax:
     - Include: <file>
   - If the file is relative, it is parsed relative to the containing file.

   Properties
   - Syntax:
     - Properties: <framework>
     - <property>: <value>
   - The values that apply to this framework are: "all" (all frameworks) and
     "it" (this integration test framework).
   - The last property definition overrides previous ones.

   Statement
   - Syntax
     - Statement: <statement text>
     - params:
       - [<parameter value>, ...]
     - param_types: [<column type>, ...]
     - output:
       - [<output value>, ...]
     - row_count: <number of rows>
     - output_types: [<column type>, ...]
     - explain: <explain plan>
     - error: [<error number>, <error message>]
   - Attributes are optional and can appear at most once
   - Only one statement in statement text
   - At least one row element in params, param_types, output, output_types
   - At least one row in params and output
   - Types for param_types and output_types listed in code below
   - The value of row_count is non-negative
   - The error message is optional
   - param_types requires params
   - Can't have error with output or row_count
   - param_types requires params
   - All rows same length for output and params
   - Same values for output row length, output_types length, and row_count
   - Same values for params row length and param_types length
   - YAML null for null value
   - !dc dc for don't care value in output
*/
public class YamlTester {

    private static final boolean DEBUG =
	Boolean.getBoolean(YamlTester.class.getName() + ".DEBUG");
    private static final Map<String, Integer> typeNameToNumber =
	new HashMap<String, Integer>();
    private static final Map<Integer, String> typeNumberToName =
	new HashMap<Integer, String>();
    static {
	addTypeNameAndNumber("BIGINT", Types.BIGINT);
	addTypeNameAndNumber("BLOB", Types.BLOB);
	addTypeNameAndNumber("BOOLEAN", Types.BOOLEAN);
	addTypeNameAndNumber("CHAR", Types.CHAR);
	addTypeNameAndNumber("CLOB", Types.CLOB);
	addTypeNameAndNumber("DATE", Types.DATE);
	addTypeNameAndNumber("DECIMAL", Types.DECIMAL);
	addTypeNameAndNumber("DOUBLE", Types.DOUBLE);
	addTypeNameAndNumber("FLOAT", Types.FLOAT);
	addTypeNameAndNumber("INTEGER", Types.INTEGER);
	addTypeNameAndNumber("NUMERIC", Types.NUMERIC);
	addTypeNameAndNumber("REAL", Types.REAL);
	addTypeNameAndNumber("SMALLINT", Types.SMALLINT);
	addTypeNameAndNumber("TIME", Types.TIME);
	addTypeNameAndNumber("TIMESTAMP", Types.TIMESTAMP);
	addTypeNameAndNumber("TINYINT", Types.TINYINT);
	addTypeNameAndNumber("VARBINARY", Types.VARBINARY);
	addTypeNameAndNumber("VARCHAR", Types.VARCHAR);
    }

    private final String filename;
    private final Reader in;
    private final Connection connection;
    private final Stack<String> includeStack = new Stack<String>();
    private int commandNumber = 0;
    private String commandName = null;
    private boolean suppressed = false;

    /**
     * Creates an instance of this class.
     *
     * @param filename the file name of the YAML input
     * @param in the YAML input
     * @param connection the JDBC connection
     */
    YamlTester(String filename, Reader in, Connection connection) {
	this.filename = filename;
	this.in = in;
	this.connection = connection;
    }

    void test() {
	test(in);
    }

    private void test(Reader in) {
	try {
	    Yaml yaml = new Yaml(new DontCareConstructor());
	    Iterator<Object> documents = yaml.loadAll(in).iterator();
	    while (documents.hasNext()) {
		++commandNumber;
		commandName = null;
		Object document = documents.next();
		List<Object> sequence =
		    nonEmptySequence(document, "command document");
		Entry<Object, Object> firstEntry = firstEntry(
		    sequence.get(0), "first element of the document");
		commandName = string(firstEntry.getKey(), "command name");
		Object value = firstEntry.getValue();
		if ("Include".equals(commandName)) {
		    includeCommand(value, sequence);
		} else if ("Properties".equals(commandName)) {
		    propertiesCommand(value, sequence);
		} else if ("Statement".equals(commandName)) {
		    statementCommand(value, sequence);
		} else {
		    fail("Unknown command: " + commandName);
		}
		if (suppressed) {
		    System.err.println(context() + "Test suppressed: exiting");
		    break;
		}
	    }
	    if (commandNumber == 0) {
		fail("Test file must not be empty");
	    }
	} catch (Throwable e) {
	    /* Add context */
	    throw initCause(new AssertionError(context() + e), e);
	}
    }

    private void includeCommand(Object value, List<Object> sequence) {
	String includeValue = string(value, "Include value");
	File include = new File(includeValue);
	if (sequence.size() > 1) {
	    throw new AssertionError(
		"The Include command does not support attributes" +
		"\nFound: " + sequence.get(1));
	}
	if (!include.isAbsolute()) {
	    String parent = filename;
	    if (!includeStack.isEmpty()) {
		parent = includeStack.peek();
	    }
	    if (parent != null) {
		include = new File(new File(parent).getParent(),
				   include.toString());
	    }
	}
	Reader in = null;
	try {
	    in = new FileReader(include);
	} catch (IOException e) {
	    throw initCause(
		new AssertionError(
		    "Problem accessing include file " + include + ": " +
		    e),
		e);
	}
	int originalCommandNumber = commandNumber;
	commandNumber = 0;
	String originalCommandName = commandName;
	commandName = null;
	try {
	    includeStack.push(includeValue);
	    test(in);
	} finally {
	    includeStack.pop();
	    commandNumber = originalCommandNumber;
	    commandName = originalCommandName;
	    try {
		in.close();
	    } catch (IOException e) {
	    }
	}
    }

    private void propertiesCommand(Object value, List<Object> sequence) {
	String engine = string(value, "Properties framework engine");
	if ("all".equals(engine) || "it".equals(engine)) {
	    for (Object elem : sequence) {
		Entry<Object, Object> entry =
		    onlyEntry(elem, "Properties entry");
		if ("suppressed".equals(entry.getKey())) {
		    suppressed = bool(entry.getValue(), "suppressed value");
		}
	    }
	}
    }

    private void statementCommand(Object value, List<Object> sequence)
	throws SQLException
    {
	new StatementCommand(value, sequence).execute();
    }

    private class StatementCommand {
	private final String statement;
	private List<List<Object>> params;
	private List<Integer> paramTypes;
	private List<List<Object>> output;
	private int rowCount = -1;
	private List<String> outputTypes;
	private boolean errorSpecified;
	private int errorNumber;
	private String errorMessage;
	private String explain;

	/**
	 * The 1-based index of the row of parameters being used for the
	 * current parameterized statement execution.
	 */
	private int paramsRow = 1;

	/**
	 * The 0-based index of the row of the output being compared with the
	 * statement output.
	 */
	private int outputRow = 0;

	StatementCommand(Object value, List<Object> sequence) {
	    statement = string(value, "Statement value");
	    for (int i = 1; i < sequence.size(); i++) {
		Entry<Object, Object> map =
		    onlyEntry(sequence.get(i), "Statement attribute");
		String attribute =
		    string(map.getKey(), "Statement attribute name");
		Object attributeValue = map.getValue();
		if ("params".equals(attribute)) {
		    parseParams(attributeValue);
		} else if ("param_types".equals(attribute)) {
		    parseParamTypes(attributeValue);
		} else if ("output".equals(attribute)) {
		    parseOutput(attributeValue);
		} else if ("row_count".equals(attribute)) {
		    parseRowCount(attributeValue);
		} else if ("output_types".equals(attribute)) {
		    parseOutputTypes(attributeValue);
		} else if ("error".equals(attribute)) {
		    parseError(attributeValue);
		} else if ("explain".equals(attribute)) {
		    parseExplain(attributeValue);
		} else {
		    fail("The " + attribute + " attribute name was not" +
			 " recognized");
		}
	    }
	    if (paramTypes != null) {
		if (params == null) {
		    fail("Cannot specify the param_types attribute without" +
			 " params attribute");
		} else {
		    assertEquals("The params_types attribute must be the same" +
				 " length as the row length of the params" +
				 " attribute:",
				 params.get(0).size(), paramTypes.size());
		}
	    }
	    if (rowCount != -1) {
		if (output != null) {
		    assertEquals("The row_count attribute must be the same" +
				 " as the length of the rows in the output"+
				 " attribute:",
				 output.get(0).size(), rowCount);
		} else if (outputTypes != null) {
		    assertEquals("The row_count attribute must be the same" +
				 " as the length of the output_types" +
				 " attribute:",
				 outputTypes.size(), rowCount);
		}
	    }
	    if (outputTypes != null) {
		if (output != null) {
		    assertEquals("The output_types attribute must be the same" +
				 " length as the length of the rows in the" +
				 " output attribute:",
				 output.get(0).size(), outputTypes.size());
		}
	    }
	    if (errorSpecified && output != null) {
		fail("Cannot specify both error and output attributes");
	    }
	    if (errorSpecified && rowCount != -1) {
		fail("Cannot specify both error and row_count attributes");
	    }
	}

	private void parseParams(Object value) {
	    assertNull("The params attribute must not appear more than once",
		       params);
	    params = rows(value, "params value");
	}

	private void parseParamTypes(Object value) {
	    assertNull(
		"The param_types attribute must not appear more than once",
		paramTypes);
	    List<String> paramTypeNames =
		nonEmptyStringSequence(value, "param_types value");
	    paramTypes = new ArrayList<Integer>(paramTypeNames.size());
	    for (String typeName : paramTypeNames) {
		Integer typeNumber = getTypeNumber(typeName);
		assertNotNull("Unknown type name in param_types: " + typeName,
			      typeNumber);
		paramTypes.add(typeNumber);
	    }
	}

	private void parseOutput(Object value) {
	    assertNull("The output attribute must not appear more than once",
		       output);
	    output = rows(value, "output value");
	}

	private void parseRowCount(Object value) {
	    assertTrue("The row_count attribute must not appear more than once",
		       rowCount == -1);
	    rowCount = integer(value, "row_count value");
	    assertTrue("The row_count value must not be negative",
		       rowCount >= 0);
	}

	private void parseOutputTypes(Object value) {
	    assertNull(
		"The output_types attribute must not appear more than once",
		paramTypes);
	    outputTypes = nonEmptyStringSequence(value, "output_types value");
	    for (String typeName : outputTypes) {
		assertNotNull("Unknown type name in output_types: " + typeName,
			      getTypeNumber(typeName));
	    }
	}

	private void parseError(Object value) {
	    assertFalse("The error attribute must not appear more than once",
			errorSpecified);
	    errorSpecified = true;
	    List<Object> errorInfo =
		nonEmptyScalarSequence(value, "error value");
	    errorNumber = integer(errorInfo.get(0), "error number");
	    if (errorInfo.size() > 1) {
		errorMessage = string(errorInfo.get(1), "error message").trim();
		assertTrue("The error attribute can have at most two" +
			   " elements",
			   errorInfo.size() < 3);
	    }
	}

	private void parseExplain(Object value) {
	    assertNull("The explain attribute must not appear more than once",
		       explain);
	    explain = string(value, "explain value").trim();
	}

	private void execute() throws SQLException {
	    if (explain != null) {
		checkExplain();
	    }
	    if (params == null) {
		Statement stmt = connection.createStatement(
		    (DEBUG ? ResultSet.TYPE_SCROLL_INSENSITIVE
		     : ResultSet.TYPE_FORWARD_ONLY),
		    ResultSet.CONCUR_READ_ONLY);
		try {
		    try {
			stmt.execute(statement);
		    } catch (SQLException e) {
			checkFailure(e);
			return;
		    }
		    checkSuccess(stmt);
		} finally {
		    stmt.close();
		}
	    } else {
		PreparedStatement stmt = connection.prepareStatement(
		    statement,
		    (DEBUG ? ResultSet.TYPE_SCROLL_INSENSITIVE
		     : ResultSet.TYPE_FORWARD_ONLY),
		    ResultSet.CONCUR_READ_ONLY);
		try {
		    int numParams = params.get(0).size();
		    for (List<Object> paramsList : params) {
			if (params.size() > 1) {
			    commandName = "Statement, params list " + paramsRow;
			}
			for (int i = 0; i < numParams; i++) {
			    Object param = paramsList.get(i);
			    if (paramTypes != null) {
				stmt.setObject(i + 1, param, paramTypes.get(i));
			    } else {
				stmt.setObject(i + 1, param);
			    }
			}
			SQLException sqlException = null;
			if (DEBUG) {
			    System.err.println(
				"Execute with params: " + paramsList);
			}
			try {
			    stmt.execute();
			} catch (SQLException e) {
			    checkFailure(e);
			    continue;
			}
			checkSuccess(stmt);
			paramsRow++;
		    }
		    commandName = "Statement";
		} finally {
		    stmt.close();
		}
	    }
	}

	private void checkExplain() throws SQLException {
	    Statement stmt = connection.createStatement();
	    try {
		stmt.execute("EXPLAIN " + statement);
		ResultSet rs = stmt.getResultSet();
		StringBuilder sb = new StringBuilder();
		int numColumns = rs.getMetaData().getColumnCount();
		while (rs.next()) {
		    for (int i = 1; i <= numColumns; i++) {
			if (i != 1) {
			    sb.append(", ");
			}
			sb.append(rs.getString(i));
		    }
		    sb.append('\n');
		}
		String got = sb.toString().trim();
		assertEquals("Explain results do not match:", explain, got);
	    } finally {
		stmt.close();
	    }
	}

	private void checkFailure(SQLException sqlException) {
	    if (!errorSpecified) {
		throw initCause(new AssertionError(
				    "Unexpected statement execution failure: " +
				    sqlException),
				sqlException);
	    }
	    if (errorNumber != sqlException.getErrorCode()) {
		throw initCause(
		    new AssertionError(
			"Unexpected error code:" +
			"\nExpected: " + errorNumber +
			"\n     got: " + sqlException.getErrorCode()),
		    sqlException);
	    }
	    if (errorMessage != null) {
		if (!errorMessage.equals(sqlException.getMessage().trim())) {
		    throw initCause(
			new AssertionError(
			    "Unexpected exception message:" +
			    "Expected: '" + errorMessage + "'"),
			sqlException);
		}
	    }
	    if (DEBUG) {
		System.err.println(
		    "Received expected exception: " + sqlException);
	    }
	}

	private void checkSuccess(Statement stmt) throws SQLException {
	    assertFalse("Statement execution succeeded, but was expected" +
			" to generate an error",
			errorSpecified);
	    ResultSet rs = stmt.getResultSet();
	    if (rs == null) {
		assertNull("Query did not produce results output", output);
		assertNull("Query did not produce results, so output_types" +
			   " are not supported",
			   outputTypes);
		if (rowCount != -1) {
		    int updateCount = stmt.getUpdateCount();
		    assertFalse("Query did not produce an update count",
				updateCount == -1);
		    outputRow += updateCount;
		    checkRowCount(rowCount, false);
		}
	    } else {
		checkResults(rs);
		assertFalse("Multiple result sets not supported",
			    stmt.getMoreResults());
	    }
	}

	/**
	 * Check if the number of rows of output seen, as measured by the
	 * outputRow field, is incorrect given the expected number of rows.
	 *
	 * @param expected the number of rows expected
	 * @param more whether there are more result rows in the current result
	 * set
	 */
	private void checkRowCount(int expected, boolean more) {
	    int got = outputRow;
	    if (more) {
		got++;
	    }
	    if (got > expected) {
		throw new AssertionError(
		    "Too many output rows:" +
		    "\nExpected: " + expected +
		    "\n     got: " + got);
	    } else if (!more &&
		       (params == null || paramsRow == params.size()) &&
		       (got < expected))
	    {
		throw new AssertionError(
		    "Too few output rows:" +
		    "\nExpected: " + expected +
		    "\n     got: " + got);
	    }
	}

	private void checkResults(ResultSet rs) throws SQLException {
	    if (DEBUG) {
		debugPrintResults(rs);
	    }
	    if (outputTypes != null && outputRow == 0) {
		checkOutputTypes(rs);
	    }
	    if (output != null) {
		ResultSetMetaData metaData = rs.getMetaData();
		int numColumns = metaData.getColumnCount();
		boolean resultsEmpty = false;
		for ( ; true; outputRow++) {
		    if (!rs.next()) {
			resultsEmpty = true;
			break;
		    } else if (outputRow >= output.size()) {
			break;
		    }
		    List<Object> row = output.get(outputRow);
		    if (outputRow == 0) {
			assertEquals("Unexpected number of columns in output:",
				     row.size(), numColumns);
		    }
		    List<Object> resultsRow = new ArrayList<Object>(row.size());
		    for (int i = 1; i <= numColumns; i++) {
			resultsRow.add(rs.getObject(i));
		    }
		    if (!rowsEqual(row, resultsRow)) {
			throw new AssertionError(
			    "Unexpected output in row " + (outputRow + 1) +
			    ":" +
			    "\nExpected: " + arrayString(row) +
			    "\n     got: " + arrayString(resultsRow));
		    }
		}
		checkRowCount(output.size(), !resultsEmpty);
	    } else if (rowCount != -1) {
		while (rs.next()) {
		    outputRow++;
		}
		checkRowCount(rowCount, false);
	    } else {
		/*
		 * Access the result data even if we aren't comparing it to
		 * anything, in case the access produces errors
		 */
		ResultSetMetaData metaData = rs.getMetaData();
		int numColumns = metaData.getColumnCount();
		while (rs.next()) {
		    for (int i = 1; i <= numColumns; i++) {
			rs.getObject(i);
		    }
		}
	    }
	}

        private void checkOutputTypes(ResultSet rs) throws SQLException {
	    ResultSetMetaData metaData = rs.getMetaData();
	    int numColumns = metaData.getColumnCount();
	    assertEquals("Wrong number of output types:",
			 outputTypes.size(), numColumns);
	    for (int i = 1; i <= numColumns; i++) {
		int columnType = metaData.getColumnType(i);
		String columnTypeName = getTypeName(columnType);
		if (columnTypeName == null) {
		    columnTypeName = "<unknown " + columnTypeName + ">";
		}
		assertEquals("Wrong output type for column " + i + ":",
			     outputTypes.get(i - 1), columnTypeName);
	    }
	}

        private boolean rowsEqual(List<Object> pattern, List<Object> row) {
	    int size = pattern.size();
	    if (size != row.size()) {
		return false;
	    }
	    for (int i = 0; i < size; i++) {
		Object patternElem = pattern.get(i);
		if (patternElem != DontCare.INSTANCE) {
		    Object rowElem = row.get(i);
		    if (patternElem == null) {
			if (rowElem != null) {
			    return false;
			}
		    } else if (!arrayElementString(patternElem).equals(
				   arrayElementString(rowElem))) {
			return false;
		    }
		}
	    }
	    return true;
	}

	private void debugPrintResults(ResultSet rs) throws SQLException {
	    System.err.println(context() + "Result output:");
	    ResultSetMetaData md = rs.getMetaData();
	    int nc = md.getColumnCount();
	    for (int i = 1; i <= nc; i++) {
		if (i != 1) {
		    System.err.print(", ");
		}
		System.err.print(md.getColumnName(i));
	    }
	    System.err.println();
	    while (rs.next()) {
		for (int i = 1; i <= nc; i++) {
		    if (i != 1) {
			System.err.print(", ");
		    }
		    System.err.print(rs.getObject(i));
		}
		System.err.println();
	    }
	    rs.beforeFirst();
	}
    }

    static String arrayString(List<Object> array) {
	if (array == null) {
	    return "null";
	}
	StringBuilder sb = new StringBuilder();
	sb.append('[');
	for (Object elem : array) {
	    if (sb.length() != 1) {
		sb.append(", ");
	    }
	    sb.append(arrayElementString(elem));
	}
	sb.append(']');
	return sb.toString();
    }

    static String arrayElementString(Object elem) {
	if (elem == null) {
	    return "null";
	} else {
	    Class elemClass = elem.getClass();
	    if (!elemClass.isArray()) {
		return elem.toString();
	    } else if (elemClass == byte[].class) {
		return Arrays.toString((byte[]) elem);
	    } else if (elemClass == short[].class) {
		return Arrays.toString((short[]) elem);
	    } else if (elemClass == int[].class) {
		return Arrays.toString((int[]) elem);
	    } else if (elemClass == long[].class) {
		return Arrays.toString((long[]) elem);
	    } else if (elemClass == char[].class) {
		return Arrays.toString((char[]) elem);
	    } else if (elemClass == float[].class) {
		return Arrays.toString((float[]) elem);
	    } else if (elemClass == double[].class) {
		return Arrays.toString((double[]) elem);
	    } else if (elemClass == boolean[].class) {
		return Arrays.toString((boolean[]) elem);
	    } else {
		/* Another type of array -- shouldn't happen */
		return elem.toString();
	    }
	}
    }

    static <T extends Throwable> T initCause(T exception, Throwable cause) {
	exception.initCause(cause);
	return exception;
    }

    static Object scalar(Object object, String desc) {
	assertThat("The " + desc + " must be a scalar",
		   object,
		   not(anyOf(instanceOf(Collection.class),
			     instanceOf(Map.class))));
	return object;
    }

    static String string(Object object, String desc) {
	assertThat("The " + desc + " must be a string",
		   object, instanceOf(String.class));
	return (String) object;
    }

    static int integer(Object object, String desc) {
	assertThat("The " + desc + " must be an integer",
		   object, instanceOf(Integer.class));
	return (Integer) object;
    }

    static boolean bool(Object object, String desc) {
	assertThat("The " + desc + " must be a boolean",
		   object, instanceOf(Boolean.class));
	return (Boolean) object;
    }

    static Map<Object, Object> map(Object object, String desc) {
	assertThat("The " + desc + " must be a map",
		   object, instanceOf(Map.class));
	return (Map<Object, Object>) object;
    }

    static Entry<Object, Object> firstEntry(Object object, String desc) {
	Map<Object, Object> map = map(object, desc);
	for (Entry<Object, Object> entry : map.entrySet()) {
	    return entry;
	}
	throw new AssertionError("The " + desc + " must not be empty");
    }

    static Entry<Object, Object> onlyEntry(Object object, String desc) {
	Map<Object, Object> map = map(object, desc);
	assertEquals("The " + desc + " must contain exactly one entry:",
		     1, map.size());
	for (Entry<Object, Object> entry : map.entrySet()) {
	    return entry;
	}
	throw new AssertionError("Not reachable");
    }

    static List<Object> sequence(Object object, String desc) {
	assertThat("The " + desc + " must be a sequence",
		   object, instanceOf(List.class));
	return (List<Object>) object;
    }

    static List<Object> nonEmptySequence(Object object, String desc) {
	List<Object> list = sequence(object, desc);
	assertFalse("The " + desc + " must not be empty", list.isEmpty());
	return list;
    }

    static List<Object> scalarSequence(Object object, String desc) {
	List<Object> list = sequence(object, desc);
	for (Object elem : list) {
	    assertThat("The element of the " + desc + " must be a scalar",
		       elem,
		       not(anyOf(instanceOf(Collection.class),
				 instanceOf(Map.class))));
	}
	return list;
    }

    static List<Object> nonEmptyScalarSequence(Object object, String desc) {
	List<Object> list = scalarSequence(object, desc);
	assertFalse("The " + desc + " must not be empty", list.isEmpty());
	return list;
    }

    static List<String> stringSequence(Object object, String desc) {
	List<Object> list = sequence(object, desc);
	List<String> strList = new ArrayList<String>(list.size());
	for (Object elem : list) {
	    assertThat("The element of the " + desc + " must be a string",
		       elem, instanceOf(String.class));
	    strList.add((String) elem);
	}
	return strList;
    }

    static List<String> nonEmptyStringSequence(Object object, String desc) {
	List<String> list = stringSequence(object, desc);
	assertFalse("The " + desc + " must not be empty", list.isEmpty());
	return list;
    }

    static List<List<Object>> rows(Object object, String desc) {
	List<Object> list = nonEmptySequence(object, desc);
	List<List<Object>> rows = new ArrayList<List<Object>>();
	int rowLength = -1;
	for (int i = 0; i < list.size(); i++) {
	    List<Object> row =
		nonEmptyScalarSequence(list.get(i), desc + " element");
	    if (i == 0) {
		rowLength = row.size();
	    } else {
		assertEquals(
		    desc + " row " + (i + 1) + " has a different" +
		    " length than previous rows:",
		    rowLength, row.size());
	    }
	    rows.add(row);
	}
	return rows;
    }

    /**
     * An object that represents a don't care value specified in the expected
     * output.
     */
    static class DontCare {
	private DontCare() { }
	static final DontCare INSTANCE = new DontCare();
	public String toString() {
	    return "!dc";
	}
    }

    /** A snakeyaml constructor that converts dc tags to DontCare.INSTANCE. */
    static class DontCareConstructor extends SafeConstructor {
	public DontCareConstructor() {
	    this.yamlConstructors.put(new Tag("!dc"), new ConstructDontCare());
	}
	private static class ConstructDontCare extends AbstractConstruct {
	    public Object construct(Node node) {
		return DontCare.INSTANCE;
	    }
	}
    }

    private static void addTypeNameAndNumber(String name, int number) {
	typeNameToNumber.put(name, number);
	typeNumberToName.put(number, name);
    }

    private static String getTypeName(int typeNumber) {
	return typeNumberToName.get(typeNumber);
    }

    private static Integer getTypeNumber(String typeName) {
	return typeNameToNumber.get(typeName);
    }

    private String context() {
	StringBuffer context = new StringBuffer();
	if (filename != null) {
	    context.append(filename);
	}
	if (!includeStack.isEmpty()) {
	    for (String include : includeStack) {
		if (context.length() != 0) {
		    context.append(", ");
		}
		context.append("Include ").append(include);
	    }
	}
	if (commandNumber != 0) {
	    if (context.length() != 0) {
		context.append(", ");
	    }
	    context.append("Command ").append(commandNumber);
	    if (commandName != null) {
		context.append(" (").append(commandName).append(')');
	    }
	}
	if (context.length() != 0) {
	    context.append(": ");
	}
	return context.toString();
    }
}
