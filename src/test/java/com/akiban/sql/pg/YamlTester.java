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
import java.util.Collection;
import java.util.HashMap;
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
public class YamlTester {

    private static String ALL_FRAMEWORKS = "all";
    private static String IT_FRAMEWORK = "it";
    private static String SUPPRESSED = "suppressed";

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
    private boolean suppressed = false;
    private int commandNumber = 0;
    private String commandKey = null;

    public YamlTester(String filename, Reader in, Connection connection) {
	this.filename = filename;
	this.in = in;
	this.connection = connection;
    }

    public void test() {
	test(in);
    }

    void test(Reader in) {
	Yaml yaml = new Yaml(new DontCareConstructor());
	for (Object document : yaml.loadAll(in)) {
	    if (suppressed) {
		System.err.println("Test suppressed: exiting");
		break;
	    }
	    ++commandNumber;
	    commandKey = null;
	    try {
		List<Object> sequence =
		    nonEmptySequence(document, "YAML document");
		Entry<Object, Object> firstEntry =
		    firstEntry(sequence.get(0),
			       "first element of the document");
		commandKey = string(firstEntry.getKey(), "command name");
		Object value = firstEntry.getValue();
		Command command;
		if ("Include".equals(commandKey)) {
		    command = new IncludeCommand(value, sequence);
		} else if ("Properties".equals(commandKey)) {
		    command = new PropertiesCommand(value, sequence);
		} else if ("Statement".equals(commandKey)) {
		    command = new StatementCommand(value, sequence);
		} else {
		    throw new AssertionError(
			"Unknown command: " + commandKey);
		}
		command.execute();
	    } catch (Throwable e) {
		throw initCause(
		    new AssertionError(context() + e.getMessage()), e);
	    }
	}
	if (commandNumber == 0) {
	    throw new AssertionError(context() + "YAML file must not be empty");
	}
    }

    abstract class Command {
	abstract void execute() throws Exception;
    }

    class PropertiesCommand extends Command {

	PropertiesCommand(Object value, List<Object> sequence) {
	    String framework = string(value, "Properties value");
	    if (framework == ALL_FRAMEWORKS || framework == IT_FRAMEWORK) {
		for (Object elem : sequence) {
		    Entry<Object, Object> entry =
			onlyEntry(elem, "Properties entry");
		    if (SUPPRESSED.equals(entry.getKey())) {
			suppressed = bool(entry.getValue(), "suppressed value");
		    }
		}
	    }
	}

	@Override
	void execute() { }
    }

    class StatementCommand extends Command {
	final String statement;
	List<List<Object>> params;
	List<Integer> paramTypes;
	List<List<Object>> output;
	List<String> outputTypes;

	/**
	 * The number of output or updated rows expected, or -1 if not
	 * specified.
	 */
	int rowCount = -1;

	boolean errorSpecified;
	int errorNumber;
	String errorMessage;

	String explain;

	/**
	 * The 1-based row of parameters being used for the current
	 * parameterized statement execution.
	 */
	int paramsRow = 1;

	/**
	 * The 0-based row of the output being compared with the statement
	 * output.
	 */
	int outputRow = 0;

	StatementCommand(Object value, List<Object> sequence) {
	    statement = string(value, "Statement value");
	    for (int i = 1; i < sequence.size(); i++) {
		Entry<Object, Object> map =
		    onlyEntry(sequence.get(i), "Statement element");
		String attribute = string(map.getKey(), "statement attribute");
		Object attributeValue = map.getValue();
		if ("params".equals(attribute)) {
		    parseParams(attributeValue);
		} else if ("param_types".equals(attribute)) {
		    parseParamTypes(attributeValue);
		} else if ("output".equals(attribute)) {
		    parseOutput(attributeValue);
		} else if ("output_types".equals(attribute)) {
		    parseOutputTypes(attributeValue);
		} else if ("error".equals(attribute)) {
		    parseError(attributeValue);
		} else if ("explain".equals(attribute)) {
		    parseExplain(attributeValue);
		} else {
		    fail("The " + attribute + " attribute was not expected");
		}
	    }
	}

	private void parseParams(Object value) {
	    List<Object> list = nonEmptySequence(value, "params value");
	    if (params == null) {
		params = new ArrayList<List<Object>>();
	    }
	    if (list.get(0) instanceof List) {
		int length = params.isEmpty() ? -1 : params.get(0).size();
		int rowNumber = 0;
		for (Object elem : list) {
		    rowNumber++;
		    List<Object> listElem =
			nonEmptyScalarSequence(elem, "params value element");
		    if (length == -1) {
			length = listElem.size();
		    } else {
			assertEquals(
			    "Parameters row " + rowNumber + " has a different" +
			    " length than previous rows:",
			    length, listElem.size());
		    }
		    params.add(listElem);
		}
	    } else {
		/* Single set of parameters */
		params.add(nonEmptyScalarSequence(list, "params value"));
	    }
	}

	private void parseParamTypes(Object value) {
	    assertNull(
		"The param_types attribute must not appear more than once",
		paramTypes);
	    List<String> paramTypeNames =
		nonEmptyStringSequence(value, "param_types value");
	    paramTypes = new ArrayList<Integer>(paramTypeNames.size());
	    for (String typeName : paramTypeNames) {
		try {
		    paramTypes.add(getTypeNumber(typeName));
		} catch (IllegalArgumentException e) {
		    throw new AssertionError(
			"Unknown type name for param_types value: " +
			typeName);
		}
	    }
	}

	private void parseOutput(Object value) {
	    assertTrue("The output attribute must not appear more than once",
		       rowCount == -1 && output == null);
	    assertFalse("The output and error attributes must not both" +
			" be specified",
			errorSpecified);
	    if (value instanceof Integer) {
		rowCount = (Integer) value;
		assertTrue("The output row count must not be negative" +
			   "\nGot: " + rowCount,
			   rowCount >= 0);
	    } else if (value instanceof List) {
		List<Object> rows = sequence(value, "output value");
		int rowLength = -1;
		output = new ArrayList<List<Object>>(rows.size());
		for (Object elem : rows) {
		    List<Object> row =
			nonEmptyScalarSequence(elem, "output row");
		    if (rowLength == -1) {
			rowLength = row.size();
		    } else {
			assertEquals("Output rows must all be the same length",
				     rowLength, row.size());
		    }
		    output.add(row);
		}
	    } else {
		fail("The output value must be a count or a sequence of row");
	    }
	}

	private void parseOutputTypes(Object value) {
	    assertNull(
		"The output_types attribute must not appear more than once",
		paramTypes);
	    outputTypes = nonEmptyStringSequence(value, "output_types value");
	}

	private void parseError(Object value) {
	    assertFalse("The error attribute must not appear more than once",
			errorSpecified);
	    assertTrue("The error and output attributes must not both" +
		       " be specified",
		       rowCount == -1 && output == null);
	    errorSpecified = true;
	    if (value instanceof Integer) {
		errorNumber = (Integer) value;
	    } else {
		List<Object> errorInfo =
		    nonEmptyScalarSequence(value, "error value");
		errorNumber = integer(errorInfo.get(0), "error number");
		if (errorInfo.size() > 1) {
		    errorMessage = string(errorInfo.get(1), "error message");
		    assertTrue("The error attribute can have at most two" +
			       " elements",
			       errorInfo.size() < 3);
		}
	    }
	}

	private void parseExplain(Object value) {
	    assertNull("The explain attribute must not appear more than once",
		       explain);
	    explain = string(value, "explain value");
	}

	@Override
	void execute() throws SQLException {
	    if (explain != null) {
		Statement stmt = connection.createStatement();
		try {
		    stmt.execute("EXPLAIN " + statement);
		    String explainResults =
			collectExplainResults(stmt.getResultSet());
		    assertEquals("Explain results do not match:",
				 explain, explainResults);
		} finally {
		    stmt.close();
		}
	    }
	    if (params == null) {
		Statement stmt = connection.createStatement(
		    ResultSet.TYPE_SCROLL_INSENSITIVE,
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
		    ResultSet.TYPE_SCROLL_INSENSITIVE,
		    ResultSet.CONCUR_READ_ONLY);
		try {
		    int numParams = params.get(0).size();
		    for (List<Object> paramsList : params) {
			if (params.size() > 1) {
			    commandKey = "Statement, params list " + paramsRow;
			}
			System.out.println(commandKey);
			for (int i = 0; i < numParams; i++) {
			    Object param = paramsList.get(i);
			    if (paramTypes != null) {
				stmt.setObject(i + 1, param, paramTypes.get(i));
			    } else {
				stmt.setObject(i + 1, param);
			    }
			}
			SQLException sqlException = null;
			System.err.println("Execute with params: " + paramsList);
			try {
			    stmt.execute();
			} catch (SQLException e) {
			    checkFailure(e);
			    continue;
			}
			checkSuccess(stmt);
			paramsRow++;
		    }
		    commandKey = "Statement";
		} finally {
		    stmt.close();
		}
	    }
	}

	private String collectExplainResults(ResultSet rs) throws SQLException {
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
	    return sb.toString();
	}

	private void checkSuccess(Statement stmt) throws SQLException {
	    assertFalse(
		"Statement execution succeeded, but was expected to fail",
		errorSpecified);
	    ResultSet rs = stmt.getResultSet();
	    if (rs == null) {
		int updateCount = stmt.getUpdateCount();
		assertFalse("Query did not produce an update count",
			    updateCount == -1);
		assertNull("Query did not produce results output",
			   output);
		assertNull("Query did not produce results, so output_types" +
			   " are not supported",
			   outputTypes);
		checkRowCount(updateCount);
	    } else {
		checkResults(rs);
		assertFalse("Multiple result sets not supported",
			    stmt.getMoreResults());
	    }
	}

	private void checkRowCount(int moreRows) {
	    if (rowCount != -1) {
		outputRow += moreRows;
		if (outputRow > rowCount) {
		    throw new AssertionError(
			"Too many output rows:" +
			"\nExpected: " + rowCount +
			"\n     got: " + outputRow);
		} else if (params == null || paramsRow == params.size()) {
		    if (outputRow < rowCount) {
			throw new AssertionError(
			    "Too few output rows:" +
			    "\nExpected: " + rowCount +
			    "\n     got: " + outputRow);
		    }
		}
	    }
	}

	private void checkResults(ResultSet rs) throws SQLException {
	    debugPrintResults(rs);
	    if (outputTypes != null && outputRow == 0) {
		ResultSetMetaData metaData = rs.getMetaData();
		int numColumns = metaData.getColumnCount();
		assertEquals("Wrong number of output types:",
			     outputTypes.size(), numColumns);
		for (int i = 1; i <= numColumns; i++) {
		    assertEquals("Wrong output type for column " + i + ":",
				 outputTypes.get(i - 1),
				 metaData.getColumnTypeName(i));
		}
	    }
	    if (rowCount != -1) {
		int c = 0;
		while (rs.next()) {
		    c++;
		}
		checkRowCount(c);
	    } else if (output != null) {
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
			    "Unexpected output in row " + (outputRow + 1) + ":" +
			    "\nExpected: " + row +
			    "\n     got: " + resultsRow);
		    }
		}
		if (outputRow < output.size()) {
		    if (resultsEmpty &&
			(params == null || paramsRow == params.size()))
		    {
			throw new AssertionError(
			    "Not enough output rows:" +
			    "\nExpected: " + output.size() +
			    "\n     got: " + (outputRow + 1));
		    }
		} else if (!resultsEmpty) {
		    throw new AssertionError(
			"Too many output rows:" +
			"\nExpected: " + output.size());
		}
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
		    if (patternElem == null ? rowElem == null
			: patternElem.equals(rowElem))
		    {
			continue;
		    } else {
			return false;
		    }
		}
	    }
	    return true;
	}

	private void debugPrintResults(ResultSet rs)
		throws SQLException
	{
	    System.err.println(context() + "Result output:");
	    printResultSet(rs);
	    rs.beforeFirst();
	}

	private void printResultSet(ResultSet rs) throws SQLException {
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
	}

	private void checkFailure(SQLException sqlException) {
	    if (!errorSpecified) {
		throw initCause(new AssertionError(
				    "Unexpected statement execution failure: " +
				    sqlException.getMessage()),
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
		if (!errorMessage.equals(sqlException.getMessage())) {
		    throw initCause(
			new AssertionError(
			    "Unexpected exception message:" +
			    "Expected: '" + errorMessage + "'"),
			sqlException);
		}
	    }
	    System.err.println("Received expected exception: " + sqlException);
	}
    }

    class IncludeCommand extends Command {
    	final String include;

	IncludeCommand(Object value, List<Object> sequence) {
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
		    include = new File(
			new File(parent).getParent(), include.toString());
		}
	    }
	    this.include = include.toString();
	    Reader in = null;
	    try {
		in = new FileReader(include);
	    } catch (IOException e) {
		throw initCause(
		    new AssertionError(
			"Problem accessing include file " + include +
			": " + e.getMessage()),
		    e);
	    }
	    int originalCommandNumber = commandNumber;
	    commandNumber = 0;
	    try {
		includeStack.push(includeValue);
		test(in);
	    } finally {
		includeStack.pop();
		commandNumber = originalCommandNumber;
		try {
		    in.close();
		} catch (IOException e) {
		}
	    }
	}

	@Override
	void execute() { }
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

    static Map<Object, Object> nonEmptyMap(Object object, String desc) {
	Map<Object, Object> map = map(object, desc);
	assertFalse("The " + desc + " must not be empty", map.isEmpty());
	return map;
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
	assertEquals("The " + desc + " must contain exactly one entry",
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

    static class DontCare {
	private DontCare() { }
	static final DontCare INSTANCE = new DontCare();
	public String toString() {
	    return "!dc";
	}
    }

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
	String name = typeNumberToName.get(typeNumber);
	if (name == null) {
	    throw new IllegalArgumentException(
		"Unknown type number: " + typeNumber);
	}
	return name;
    }

    private static int getTypeNumber(String typeName) {
	Integer number = typeNameToNumber.get(typeName);
	if (number == null) {
	    throw new IllegalArgumentException(
		"Unknown type name: " + typeName);
	}
	return number;
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
	    if (commandKey != null) {
		context.append(" (").append(commandKey).append(')');
	    }
	}
	if (context.length() != 0) {
	    context.append(": ");
	}
	return context.toString();
    }
}
