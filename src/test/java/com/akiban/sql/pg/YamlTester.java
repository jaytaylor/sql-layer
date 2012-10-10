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

import static com.akiban.util.AssertUtils.assertCollectionEquals;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.regex.Pattern;

import com.akiban.server.types3.Types3Switch;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

/**
 * A utility for testing SQL access over a Postgres server connection based on
 * the contents of a YAML file.
 */
/* Here's an overview of the syntax of the YAML file.

  General:

   - One or more YAML documents
   - Each document is a sequence whose first element is a map
   - Key of first element's map is a command: a string with an uppercase first
     character

   Commands:

   Include
   - Syntax:
     - Include: <file>
   - If the file is relative, it is parsed relative to the referring file
   - If the file argument is missing or null, the command will be ignored

   Properties
   - Syntax:
     - Properties: <framework engine>
     - <property>: <value>
   - The frame engines that apply to this engine are: "all" (all frameworks) and
     "it" (this integration test framework engine)
   - A new property definition overrides any previous ones

   CreateTable
   - Syntax:
     - CreateTable: <table name> <create table arguments>...
     - error: [<error code>, <error message>]
   - The error message is optional
   - If the error code is missing or null, then the attribute and its value
     will be ignored
   - This command has the same behavior as would specifying a CREATE TABLE
     statement with a Statement command, but it lets the test framework know
     which tables have been created, so it can drop them after the test is
     completed

   Statement
   - Syntax
     - Statement: <statement text>
     - params: [[<parameter value>, ...], ...]
     - param_types: [<column type>, ...]
     - output: [[<output value>, ...], ...]
     - output_ordered: [[<output value>, ...], ...]
     - row_count: <number of rows>
     - output_types: [<column type>, ...]
     - explain: <explain plan>
     - error: [<error code>, <error message>]
     - warnings_count: <count>
     - warnings: [[<warning code>, <warning message], ...]
   - If the statement text is missing or null, the command will be ignored
   - Attributes are optional and can appear at most once
   - If the value of any attribute is missing or null, then the attribute and
     its value will be ignored
   - Only one statement in statement text (not checked)
   - At least one row element in params, param_types, output, output_types
   - At least one row in params and output
   - Types for param_types and output_types listed in code below
   - param_types requires params
   - All rows same length for output and params
   - Same values for output row length, output_types length, and row_count
   - Same values for params row length and param_types length
   - The value of row_count is non-negative
   - The error message is optional
   - Can't have error with output or row_count
   - YAML null for null value
   - !dc dc for don't care value in output
   - !re regular-expression for regular expression patterns that should match
     output, error codes, error messages, warnings, or explain output
   - The statement text should not create a table -- use the CreateTable
     command for that purpose
   - output_ordered: does a sort on the expected and actual during comparison  
   - output_ordered: does a sort on the expected and actual during comparison
   - Warnings include statement warnings followed by result set warnings for
     each output row
   - The warning message is optional
   
   BulkLoad is not supported in IT level tests
   if used, please suppress the IT level calls or place tests in AAS directly
   
   - JMX: <objectName>   (i.e com.akiban:type=IndexStatistics)
   ** Only one allowed of the following three (3) per command set
   - set: <set method>
   - method: <method>
   - get: <get method>
   
   - params: [<parameter value>, ...]
   - output: [[<output value>, ...], ...]



*/
class YamlTester {

    private static final boolean DEBUG = Boolean.getBoolean("test.DEBUG");
    private static final Map<String, Integer> typeNameToNumber = new HashMap<String, Integer>();
    private static final Map<Integer, String> typeNumberToName = new HashMap<Integer, String>();
    static {
	addTypeNameAndNumber("VARBINARY", Types.BINARY); // name to number overwritten below
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

    /** Matches all engines. */
    private static final String ALL_ENGINE = "all";
    /** Matches the IT engine. */
    private static final String IT_ENGINE = "it";
    /** Matches the newtypes "engine." */
    private static final String NEWTYPES_ENGINE = "newtypes";

    /** Compare toString values of arguments, ignoring case. */
    private static final Comparator<? super Object> COMPARE_IGNORE_CASE =
        new Comparator<Object>() {
            public int compare(Object x, Object y) {
                return String.valueOf(x).compareToIgnoreCase(String.valueOf(y));
            }
        };

    private final String filename;
    private final Reader in;
    private final Connection connection;
    private final Stack<String> includeStack = new Stack<String>();
    private int commandNumber = 0;
    private String commandName = null;
    private boolean suppressed = false;
    private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat(
	    "yyyy-MM-dd");
    private static final DateFormat DEFAULT_DATETIME_FORMAT = new SimpleDateFormat(
	    "yyyy-MM-dd HH:mm:ss.S z");

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

    /** Test the input specified in the constructor. */
    void test() {
	test(in);
    }

    private void test(Reader in) {
    List<Object> sequence = null;
	try {
	    Yaml yaml = new Yaml(new RegisterTags());
	    Iterator<Object> documents = yaml.loadAll(in).iterator();
	    while (documents.hasNext()) {
		++commandNumber;
		commandName = null;
		Object document = documents.next();
		sequence = nonEmptySequence(document,
			"command document");
		Entry<Object, Object> firstEntry = firstEntry(sequence.get(0),
			"first element of the document");
		commandName = string(firstEntry.getKey(), "command name");
		Object value = firstEntry.getValue();
		if ("Include".equals(commandName)) {
		    includeCommand(value, sequence);
		} else if ("Properties".equals(commandName)) {
		    propertiesCommand(value, sequence);
		} else if ("CreateTable".equals(commandName)) {
		    createTableCommand(value, sequence);
		} else if ("DropTable".equals(commandName)) {
		    dropTableCommand(value, sequence);
		} else if ("Statement".equals(commandName)) {
		    statementCommand(value, sequence);
		} else if ("Message".equals(commandName)) {
	        messageCommand(value);
		} else if ("Bulkload".equals(commandName)) {
                    bulkloadCommand(value, sequence); 
                } else if ("JMX".equals(commandName)) {
                    jmxCommand(value, sequence);
		} else {
		    fail("Unknown command: " + commandName);
		}
		if (suppressed) {
		    System.err.println(context(null) + "Test suppressed: exiting");
		    break;
		}
	    }
	    if (commandNumber == 0) {
		fail("Test file must not be empty");
	    }
	} catch (ContextAssertionError e) {
	    throw e;
	} catch (Throwable e) {
	    /* Add context */
	    throw new ContextAssertionError(String.valueOf(sequence), e.toString(), e);
	}
    }

    private void bulkloadCommand(Object value, List<Object> sequence) {
        // ignore this command.  Not meant for ITs, only system testing
        throw new ContextAssertionError(null, "Bulk Load command is not supported in ITs");
    }

    private void includeCommand(Object value, List<Object> sequence) {
	if (value == null) {
	    return;
	}
	String includeValue = string(value, "Include value");
	File include = new File(includeValue);
	if (sequence.size() > 1) {
	    throw new ContextAssertionError(
            includeValue,
		    "The Include command does not support attributes"
			    + "\nFound: " + sequence.get(1));
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
	    in = new InputStreamReader(new FileInputStream(include), "UTF-8");
	} catch (IOException e) {
	    throw new ContextAssertionError(includeValue, "Problem accessing include file "
		    + include + ": " + e, e);
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

    private void messageCommand(Object value) {
        String message = string(value, "Message");
        System.err.println("FTS Message: " + message);
    }

    private void propertiesCommand(Object value, List<Object> sequence) {
	String engine = string(value, "Properties framework engine");
	if (ALL_ENGINE.equals(engine) || IT_ENGINE.equals(engine)) {
	    for (Object elem : sequence) {
		Entry<Object, Object> entry = onlyEntry(elem,
			"Properties entry");
		if ("suppressed".equals(entry.getKey())) {
		    suppressed = bool(entry.getValue(), "suppressed value");
		}
	    }
	}
    }

    /** Implements common behavior of commands that execute statements. */
    private abstract class AbstractStatementCommand {
	final String statement;
	boolean errorSpecified;
	Object errorCode;
	Object errorMessage;
	boolean sorted;

	/** Handle a statement with the specified statement text. */
	AbstractStatementCommand(String statement) {
	    this.statement = statement;
	}

	/** Parse an error attribute with the specified value. */
	void parseError(Object value) {
	    if (value == null) {
		return;
	    }
	    assertFalse("The error attribute must not appear more than once",
		    errorSpecified);
	    errorSpecified = true;
	    List<Object> errorInfo = nonEmptyScalarSequence(value,
		    "error value");
	    errorCode = scalar(errorInfo.get(0), "error code");
	    if (errorInfo.size() > 1) {
		errorMessage = scalar(errorInfo.get(1), "error message");
                assertTrue("The error attribute can have at most two"
			+ " elements", errorInfo.size() < 3);
	    }
	}

	/**
	 * Check the specified exception against the error attribute specified
	 * earlier, if any.
	 */
	void checkFailure(SQLException sqlException) {
	    if (DEBUG) {
		System.err.println("Generated error code: "
			+ sqlException.getSQLState() + "\nException: "
			+ sqlException);
	    }
	    if (!errorSpecified) {
		throw new ContextAssertionError(
            statement,
			"Unexpected statement execution failure: "
				+ sqlException, sqlException);
	    }
            checkExpected("error code", errorCode, sqlException.getSQLState());
	    if (errorMessage != null) {
                checkExpected("error message", errorMessage,
                              sqlException.getMessage());
	    }
	}

    
    }

    /** Represents an SQL warning. */
    private static class Warning implements CompareExpected {
        /** The SQL state -- warning code */
        final Object code;
        /** The warning message */
        final Object message;
        Warning(Object code, Object message) {
            this.code = code;
            this.message = message;
        }
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(code);
            if (message != null) {
                sb.append(", '").append(message).append("'");
            }
            sb.append("]");
            return sb.toString();
        }
        public boolean compareExpected(Object actual) {
            if (!(actual instanceof Warning)) {
                return false;
            }
            Warning warning = (Warning) actual;
            /* Require the codes to match */
            if (!expected(code, warning.code)) {
                return false;
            }
            /*
             * Only require the message to match if the pattern -- this object
             * -- specifies a message
             */
            return message == null || expected(message, warning.message);
        }
    }

    private void createTableCommand(Object value, List<Object> sequence)
	    throws SQLException {
	new CreateTableCommand(value, sequence).execute();
    }

    private class CreateTableCommand extends AbstractStatementCommand 
    {
        private Object warningsCount;
        private List<Warning> warnings;
        
        CreateTableCommand(Object value, List<Object> sequence) {
	    super("CREATE TABLE " + string(value, "CreateTable argument"));
	    for (int i = 1; i < sequence.size(); i++) {
		Entry<Object, Object> map = onlyEntry(sequence.get(i),
			"CreateTable attribute");
		String attribute = string(map.getKey(),
			"CreateTable attribute name");
		Object attributeValue = map.getValue();
		if ("error".equals(attribute))
		    parseError(attributeValue);
                else if ("warnings_count".equals(attribute)) 
                    warningsCount = parseWarningsCount(attributeValue, warningsCount);
                else if ("warnings".equals(attribute)) 
                    warnings = parseWarnings(attributeValue, warnings);
                else 
		    fail("The '" + attribute + "' attribute name was not"
			    + " recognized");

                if (warnings != null && warningsCount != null 
                                     && !expected(warningsCount, warnings.size()))
                    fail("Warnings count " + warningsCount
                         + " does not match " + warnings.size()
                         + ", which is the number of warnings");
	    }
	}

	void execute() throws SQLException {
	    Statement stmt = connection.createStatement();
	    if (DEBUG) {
		System.err.println("Executing statement: " + statement);
	    }
	    try {
		stmt.execute(statement);
		if (DEBUG) {
		    System.err.println("Statement executed successfully");
		}
                
	    } catch (SQLException e) {
		if (DEBUG) {
		    System.err.println("Generated error code: "
			    + e.getSQLState() + "\nException: " + e);
		}
		checkFailure(e);
		return;
	    }
            checkSuccess(stmt, errorSpecified, warnings, warningsCount);
	}
    }

    
    private void dropTableCommand(Object value, List<Object> sequence) throws SQLException {
        new DropTableCommand(value, sequence).execute();
    }
    

    private class DropTableCommand extends AbstractStatementCommand
    {
                
        private Object warningsCount;
        private List<Warning> warnings;
	
        DropTableCommand(Object value, List<Object> sequence)
        {
	    super("DROP TABLE " + string(value, "DropTable argument"));
            for (int i = 1; i < sequence.size(); i++) {
		Entry<Object, Object> map = onlyEntry(sequence.get(i),
			"DropTable attribute");
		String attribute = string(map.getKey(),
			"CreateTable attribute name");
		Object attributeValue = map.getValue();
		if ("error".equals(attribute))
		    parseError(attributeValue);
                else if ("warnings_count".equals(attribute)) 
                    warningsCount = parseWarningsCount(attributeValue, warningsCount);
                else if ("warnings".equals(attribute)) 
                    warnings = parseWarnings(attributeValue, warnings);
                else 
		    fail("The '" + attribute + "' attribute name was not"
			    + " recognized");

                if (warnings != null && warningsCount != null 
                                     && !expected(warningsCount, warnings.size()))
                    fail("Warnings count " + warningsCount
                         + " does not match " + warnings.size()
                         + ", which is the number of warnings");
	    }
	}

	void execute() throws SQLException {
	    Statement stmt = connection.createStatement();
	    if (DEBUG) {
		System.err.println("Executing statement: " + statement);
	    }
	    try {
		stmt.execute(statement);
		if (DEBUG) {
		    System.err.println("Statement executed successfully");
		}
                
	    } catch (SQLException e) {
		if (DEBUG) {
		    System.err.println("Generated error code: "
			    + e.getSQLState() + "\nException: " + e);
		}
		checkFailure(e);
		return;
	    }
            checkSuccess(stmt, errorSpecified, warnings, warningsCount);
	}
    }

    private void statementCommand(Object value, List<Object> sequence)
	    throws SQLException {
	if (value != null) {
	    new StatementCommand(value, sequence).execute();
	}
    }

    private class StatementCommand extends AbstractStatementCommand {
	private List<List<Object>> params;
	private List<Integer> paramTypes;
	private List<List<Object>> output;
	private int rowCount = -1;
	private List<String> outputTypes;
	private Object explain;
        private Object warningsCount;
        private List<Warning> warnings;

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
	    super(string(value, "Statement value"));
	    for (int i = 1; i < sequence.size(); i++) {
		Entry<Object, Object> map = onlyEntry(sequence.get(i),
			"Statement attribute");
		String attribute = string(map.getKey(),
			"Statement attribute name");
		Object attributeValue = map.getValue();
		if ("params".equals(attribute)) {
		    parseParams(attributeValue);
		} else if ("param_types".equals(attribute)) {
		    parseParamTypes(attributeValue);
		} else if ("output".equals(attribute)) {
		    this.sorted = false;
		    parseOutput(attributeValue);
		} else if ("output_ordered".equals(attribute)) {
		    this.sorted = true;
		    parseOutput(attributeValue);
		} else if ("row_count".equals(attribute)) {
		    parseRowCount(attributeValue);
		} else if ("output_types".equals(attribute)) {
		    parseOutputTypes(attributeValue);
		} else if ("error".equals(attribute)) {
		    parseError(attributeValue);
		} else if ("explain".equals(attribute)) {
		    parseExplain(attributeValue);
                } else if ("warnings_count".equals(attribute)) {
                   warningsCount = parseWarningsCount(attributeValue, warningsCount);
                } else if ("warnings".equals(attribute)) {
                    warnings = parseWarnings(attributeValue, warnings);
		} else {
		    fail("The '" + attribute + "' attribute name was not"
			    + " recognized");
		}
	    }
	    if (paramTypes != null) {
		if (params == null) {
		    fail("Cannot specify the param_types attribute without"
			    + " params attribute");
		} else {
		    assertEquals("The params_types attribute must be the same"
			    + " length as the row length of the params"
			    + " attribute:", params.get(0).size(),
			    paramTypes.size());
		}
	    }
	    if (rowCount != -1) {
		if (output != null) {
		    assertEquals("The row_count attribute must be the same"
			    + " as the length of the rows in the output"
			    + " attribute:", output.size(), rowCount);
		} else if (outputTypes != null) {
		    assertEquals("The row_count attribute must be the same"
			    + " as the length of the output_types"
			    + " attribute:", outputTypes.size(), rowCount);
		}
	    }
	    if (outputTypes != null) {
		if (output != null) {
		    assertEquals("The output_types attribute must be the same"
			    + " length as the length of the rows in the"
			    + " output attribute:", output.get(0).size(),
			    outputTypes.size());
		}
	    }
	    if (errorSpecified && output != null) {
		fail("Cannot specify both error and output attributes");
	    }
	    if (errorSpecified && rowCount != -1) {
		fail("Cannot specify both error and row_count attributes");
	    }
            if (warnings != null &&
                warningsCount != null &&
                !expected(warningsCount, warnings.size()))
            {
                fail("Warnings count " + warningsCount +
                     " does not match " + warnings.size() +
                     ", which is the number of warnings");
            }
	}

	private void parseParams(Object value) {
	    if (value == null) {
		return;
	    }
	    assertNull("The params attribute must not appear more than once",
		    params);
	    params = rows(value, "params value");
	}

	private void parseParamTypes(Object value) {
	    if (value == null) {
		return;
	    }
	    assertNull(
		    "The param_types attribute must not appear more than once",
		    paramTypes);
	    List<String> paramTypeNames = nonEmptyStringSequence(value,
		    "param_types value");
	    paramTypes = new ArrayList<Integer>(paramTypeNames.size());
	    for (String typeName : paramTypeNames) {
		Integer typeNumber = getTypeNumber(typeName);
		assertNotNull("Unknown type name in param_types: " + typeName,
			typeNumber);
		paramTypes.add(typeNumber);
	    }
	}

	private void parseOutput(Object value) {
	    if (value == null) {
		return;
	    }
	    assertNull("The output attribute must not appear more than once",
		    output);
	    output = rows(value, "output value");
	}

	private void parseRowCount(Object value) {
	    if (value == null) {
		return;
	    }
	    assertTrue(
		    "The row_count attribute must not appear more than once",
		    rowCount == -1);
	    rowCount = integer(value, "row_count value");
	    assertTrue("The row_count value must not be negative",
		    rowCount >= 0);
	}

	private void parseOutputTypes(Object value) {
	    if (value == null) {
		return;
	    }
	    assertNull(
		    "The output_types attribute must not appear more than once",
		    outputTypes);
	    outputTypes = nonEmptyStringSequence(value, "output_types value");
	    for (String typeName : outputTypes) {
		assertNotNull("Unknown type name in output_types: " + typeName,
			getTypeNumber(typeName));
	    }
	}

	private void parseExplain(Object value) {
	    if (value == null) {
		return;
	    }
	    assertNull("The explain attribute must not appear more than once",
		    explain);
	    explain = scalar(value, "explain value");
	}

	void execute() throws SQLException {
	    if (explain != null) {
		checkExplain();
	    }
	    if (params == null) {
		Statement stmt = connection.createStatement();
		try {
		    if (DEBUG) {
			System.err.println("Executing statement: " + statement);
		    }
		    try {
			stmt.execute(statement);
		    } catch (SQLException e) {
			checkFailure(e);
			return;
		    }
		    checkSuccess(stmt, sorted);
		} finally {
		    stmt.close();
		}
	    } else {
		PreparedStatement stmt = connection.prepareStatement(statement);
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
			if (DEBUG) {
			    System.err
				    .println("Executing statement: "
					    + statement + "\nParameters: "
					    + paramsList);
			}
			try {
			    stmt.execute();
			} catch (SQLException e) {
			    checkFailure(e);
			    continue;
			}
			checkSuccess(stmt, sorted);
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
                checkExpected("explain output", explain, got);
	    } finally {
		stmt.close();
	    }
	}

	private void checkSuccess(Statement stmt, boolean sorted)
		throws SQLException {
	    assertFalse("Statement execution succeeded, but was expected"
		    + " to generate an error", errorSpecified);
	    ResultSet rs = stmt.getResultSet();
	    if (rs == null) {
		assertNull("Query did not produce results output", output);
		assertNull("Query did not produce results, so output_types"
			+ " are not supported", outputTypes);
		if (rowCount != -1) {
		    int updateCount = stmt.getUpdateCount();
		    assertFalse("Query did not produce an update count",
			    updateCount == -1);
		    outputRow += updateCount;
		    checkRowCount(rowCount, false);
		}
                List<Warning> reportedWarnings = new ArrayList<Warning>();
                collectWarnings(stmt.getWarnings(), reportedWarnings);
                checkWarnings(reportedWarnings);
	    } else {
		checkResults(rs, sorted);
		assertFalse("Multiple result sets not supported",
			stmt.getMoreResults());
	    }
	}

        /**
         * Add the warning message from the specified warning, as well as any
         * additional warnings linked via the getNextWarning method, to the
         * list of messages.
         */
        private void collectWarnings(SQLWarning warning, List<Warning> messages)
        {
            while (warning != null) {
                messages.add(
                    new Warning(warning.getSQLState(), warning.getMessage()));
                warning = warning.getNextWarning();
            }
        }

        private void checkWarnings(List<Warning> reportedWarnings) {
            if (DEBUG && !reportedWarnings.isEmpty()) {
                System.err.println("Statement warnings: " + reportedWarnings);
            }
            if (warningsCount != null) {
                checkExpected(
                    "warnings count", warningsCount, reportedWarnings.size());
            }
            if (warnings == null) {
                return;
            }
            if (reportedWarnings.isEmpty()) {
                if (!warnings.isEmpty()) {
                    fail("No warnings were reported, but expected warnings: " +
                         warnings);
                }
            } else {
                if (warnings.isEmpty()) {
                    fail("Warnings were reported but none were expected: " +
                         warnings);
                }
                checkExpectedList("Warnings", warnings, reportedWarnings);
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
		throw new ContextAssertionError(statement, "Too many output rows:"
			+ "\nExpected: " + expected + "\n     got: " + got);
	    } else if (!more && (params == null || paramsRow == params.size())
		    && (got < expected)) {
		throw new ContextAssertionError(statement, "Too few output rows:"
			+ "\nExpected: " + expected + "\n     got: " + got);
	    }
	}

	private void checkResults(ResultSet rs, boolean sorted)
		throws SQLException {
	    if (outputTypes != null && outputRow == 0) {
		checkOutputTypes(rs);
	    }
	    if (DEBUG) {
		System.err.println("Statement output:");
	    }
	    if (output != null) {
		ResultSetMetaData metaData = rs.getMetaData();
		int numColumns = metaData.getColumnCount();
		boolean resultsEmpty = false;
                List<List<Object>> resultsList = new ArrayList<List<Object>>();
                List<Warning> reportedWarnings = new ArrayList<Warning>();
                Statement stmt = rs.getStatement();
                assert stmt != null;
                collectWarnings(stmt.getWarnings(), reportedWarnings);
                for (int i = 0; true; i++) {
                    if (!rs.next()) {
                        resultsEmpty = true;
                        break;
                    } else if (i >= output.size()) {
                        break;
                    }
                    List<Object> row = output.get(i);
                    if (i == 0) {
                        assertEquals("Unexpected number of columns in output:",
                                     row.size(), numColumns);
                    }
                    List<Object> resultsRow = new ArrayList<Object>(row.size());
                    for (int j = 1; j <= numColumns; j++) {
                        resultsRow.add(rs.getObject(j));
                    }
                    resultsList.add(resultsRow);
                    collectWarnings(rs.getWarnings(), reportedWarnings);
                    if (DEBUG) {
                        System.err.println(arrayString(resultsRow));
                    }
                }
                if (sorted) {
                    Collections.sort(output, COMPARE_IGNORE_CASE);
                    Collections.sort(resultsList, COMPARE_IGNORE_CASE);
                }
                int i = 0;
                for ( ; true; outputRow++, i++) {
                    if (outputRow >= output.size()) {
                        if (i < resultsList.size()) {
                            resultsEmpty = false;
                        }
                        break;
                    } else if (i >= resultsList.size()) {
                        break;
                    }
                    List<Object> row = output.get(outputRow);
                    List<Object> resultsRow = resultsList.get(i);
                    if (i >= resultsList.size()) {
                        break;
                    } else if (!rowsEqual(row, resultsRow)) {
                        throw new ContextAssertionError(
                            statement,
                            "Unexpected output in row " + (outputRow + 1) + ":"
                            + "\nExpected: " + arrayString(row)
                            + "\n     got: "
                            + arrayString(resultsRow));
                    }
                }
                checkRowCount(output.size(), !resultsEmpty);
	    } else {
		ResultSetMetaData metaData = rs.getMetaData();
		int numColumns = metaData.getColumnCount();
		List<Object> resultsRow = DEBUG ? new ArrayList<Object>(
			numColumns) : null;
                List<Warning> reportedWarnings = new ArrayList<Warning>();
                Statement stmt = rs.getStatement();
                assert stmt != null;
                collectWarnings(stmt.getWarnings(), reportedWarnings);
                while (rs.next()) {
		    outputRow++;
		    for (int i = 1; i <= numColumns; i++) {
			Object result = rs.getObject(i);
			if (DEBUG) {
			    resultsRow.add(result);
			}
		    }
		    if (DEBUG) {
			System.err.println(arrayString(resultsRow));
			resultsRow.clear();
		    }
                    collectWarnings(rs.getWarnings(), reportedWarnings);
		}
		if (rowCount != -1) {
		    checkRowCount(rowCount, false);
		}
                checkWarnings(reportedWarnings);
	    }
	}
	
	private boolean rowsEqual(List<Object> pattern, List<Object> row) {
	    int size = pattern.size();
	    if (size != row.size()) {
	        return false;
	    }
	    for (int i = 0; i < size; i++) {
	        Object patternElem = pattern.get(i);
	        Object rowElem = row.get(i);
	        if (!expected(patternElem, rowElem)) {
	            return false;
	        }
	    }
	    return true;
	}

	private void checkOutputTypes(ResultSet rs) throws SQLException {
	    ResultSetMetaData metaData = rs.getMetaData();
	    int numColumns = metaData.getColumnCount();
	    assertEquals("Wrong number of output types:", outputTypes.size(),
		    numColumns);
	    for (int i = 1; i <= numColumns; i++) {
		int columnType = metaData.getColumnType(i);
		String columnTypeName = getTypeName(columnType);
		if (columnTypeName == null) {
		    columnTypeName = "<unknown "
			    + metaData.getColumnTypeName(i) + " (" + columnType
			    + ")>";
		}
		assertEquals("Wrong output type for column " + i + ":",
			outputTypes.get(i - 1), columnTypeName);
	    }
	}
    }
    
    //----------- static helpers -----------------

    static Object stripWARN (Object msg)
    {
        
        if (msg instanceof String)
        {
            String st = (String)msg;
            if (st.startsWith("WARN:  "))
                return st.substring(7);
        }
        return msg;
    }

    static void checkSuccess(Statement stmt, boolean errorSpecified, List<Warning> warnings, Object warningsCount) throws SQLException
    {
        assertFalse("Statement execution succeeded, but was expected"
                    + " to generate an error", errorSpecified);

        List<Warning> reportedWarnings = new ArrayList<Warning>();
        collectWarnings(stmt.getWarnings(), reportedWarnings);
        checkWarnings(reportedWarnings, warnings, warningsCount);
    }

    static Object parseWarningsCount(Object value, Object warningsCount)
    {
        if (value == null)
            return null;
        assertNull("The warnings_count attribute must not appear more than once",
                   warningsCount);
        return warningsCount = scalar(value, "warningsCount");
    }

    static List<Warning>  parseWarnings(Object value, List<Warning> warnings)
    {
        if (value == null)
            return null;
        assertNull("The warnings attribute must not appear more than once",
                   warnings);

        List<Object> list = nonEmptySequence(value, "warnings");
        warnings = new ArrayList<Warning>();
        for (int i = 0; i < list.size(); i++)
        {
            List<Object> element = nonEmptyScalarSequence(
                    list.get(i), "warnings element " + i);
            assertFalse("Warnings element " + i + " is empty",
                        element.isEmpty());
            assertFalse("Warnings element " + i + " has more than two elements",
                        element.size() > 2);
            warnings.add(new Warning(
                    element.get(0),
                    element.size() > 1 ? stripWARN(element.get(1)) : null));
        }
        return warnings;
    }

    private static void collectWarnings(SQLWarning warning, List<Warning> messages)
    {
        while (warning != null)
        {
            messages.add(new Warning(warning.getSQLState(), warning.getMessage()));
            warning = warning.getNextWarning();
        }
    }

    private static void checkWarnings(List<Warning> reportedWarnings, List<Warning> warnings, Object warningsCount)
    {
        if (DEBUG && !reportedWarnings.isEmpty())
            System.err.println("Statement warnings: " + reportedWarnings);

        if (warningsCount != null)
            checkExpected("warnings count", warningsCount, reportedWarnings.size());
        if (warnings == null)
            return;
        if (reportedWarnings.isEmpty())
        {
            if (!warnings.isEmpty())
                fail("No warnings were reported, but expected warnings: " + warnings);
        }
        else
        {
            if (warnings.isEmpty())
                fail("Warnings were reported but none were expected: " + warnings);
            checkExpectedList("Warnings", warnings, reportedWarnings);
        }
    }
    
    static void checkExpectedList(String description, List<?> expected,
                                  List<?> actual)
    {
        int expectedSize = expected.size();
        int actualSize = actual.size();
        if (expectedSize != actualSize) {
            fail(description + " should have " + expectedSize +
                 " elements, but has " + actualSize + ":" +
                 "\nExpected: " + expected +
                 "\n     got: " + actual);
        }
        for (int i = 0; i < expectedSize; i++) {
            Object expectedElement = expected.get(i);
            Object actualElement = actual.get(i);
            if (!expected(expectedElement, actualElement)) {
                fail("Incorrect value for element " + i + " of " +
                     description + ":" +
                     "\nExpected: " + expected +
                     "\n     got: " + actual);
            }
        }
    }

    static void checkExpected(String description, Object expected,
                              Object actual)
    {
        if (!expected(expected, actual)) {
            fail("Incorrect " + description + ":" +
                 "\nExpected: " + expected +
                 "\n     got: " + actual);
        }
    }

    static boolean expected(Object expected, Object actual) {
        if (expected instanceof CompareExpected) {
            return ((CompareExpected) expected).compareExpected(actual);
        } else if (expected == null) {
            return actual == null;
        } else {
            String expectedString = objectToString(expected);
            String actualString = objectToString(actual);
     
            return expectedString.equals(actualString);
        }
    }

    static String arrayString(List<Object> array) {
	if (array == null) {
	    return "null";
	}
	StringBuilder sb = new StringBuilder();
	sb.append('[');
        for (int i = 0; i < array.size(); i++) {
            Object elem = array.get(i);
            if (i != 0) {
		sb.append(", ");
	    }
	    sb.append(objectToString(elem));
	}
	sb.append(']');
	return sb.toString();
    }

    static String objectToString(Object object) {
	if (object == null) {
	    return "null";
	} else {
	    Class objectClass = object.getClass();
	    if (!objectClass.isArray()) {
		return object.toString();
	    } else if (objectClass == byte[].class) {
		return Arrays.toString((byte[]) object);
	    } else if (objectClass == short[].class) {
		return Arrays.toString((short[]) object);
	    } else if (objectClass == int[].class) {
		return Arrays.toString((int[]) object);
	    } else if (objectClass == long[].class) {
		return Arrays.toString((long[]) object);
	    } else if (objectClass == char[].class) {
		return Arrays.toString((char[]) object);
	    } else if (objectClass == float[].class) {
		return Arrays.toString((float[]) object);
	    } else if (objectClass == double[].class) {
		return Arrays.toString((double[]) object);
	    } else if (objectClass == boolean[].class) {
		return Arrays.toString((boolean[]) object);
	    } else {
		/* Another type of array -- shouldn't happen */
		return object.toString();
	    }
	}
    }

    static Object scalar(Object object, String desc) {
	assertThat("The " + desc + " must be a scalar", object,
		not(anyOf(instanceOf(Collection.class), instanceOf(Map.class))));
	return object;
    }

    static String string(Object object, String desc) {
	assertThat("The " + desc + " must be a string", object,
		instanceOf(String.class));
	return (String) object;
    }

    static int integer(Object object, String desc) {
	assertThat("The " + desc + " must be an integer", object,
		instanceOf(Integer.class));
	return (Integer) object;
    }

    static boolean bool(Object object, String desc) {
	assertThat("The " + desc + " must be a boolean", object,
		instanceOf(Boolean.class));
	return (Boolean) object;
    }

    static Map<Object, Object> map(Object object, String desc) {
	assertThat("The " + desc + " must be a map", object,
		instanceOf(Map.class));
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
	assertEquals("The " + desc + " must contain exactly one entry:", 1,
		map.size());
	for (Entry<Object, Object> entry : map.entrySet()) {
	    return entry;
	}
	throw new AssertionError("Not reachable");
    }

    static List<Object> sequence(Object object, String desc) {
	assertThat("The " + desc + " must be a sequence", object,
		instanceOf(List.class));
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
	    assertThat(
		    "The element of the " + desc + " must be a scalar",
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
	    List<Object> row = nonEmptyScalarSequence(list.get(i), desc
		    + " element");
	    if (i == 0) {
		rowLength = row.size();
	    } else {
		assertEquals(desc + " row " + (i + 1) + " has a different"
			+ " length than previous rows:", rowLength, row.size());
	    }
	    rows.add(row);
	}
	return rows;
    }

    /** Support comparing an expected value to an actual value. */
    interface CompareExpected {
	/**
	 * Compares an actual value with the expected value represented by this
	 * object.
	 *
	 * @param actual the actual value
	 * @return whether the actual value matches the expected value
	 */
	boolean compareExpected(Object actual);
    }

    /** Support comparing this object to expected output. */
    interface OutputComparator {
        /**
         * Compares the specified output with this object, which represents the
         * expected output.
         *
         * @param output the output
         * @return whether the output matches the expected output
         */
        boolean compareOutput(Object output);
    }
    
    /**
     * An object that represents a don't care value specified as an expected
     * value.
     */
    static class DontCare implements CompareExpected {
	static final DontCare INSTANCE = new DontCare();

	private DontCare() {
	}

	@Override
	public boolean compareExpected(Object actual) {
	    return true;
	}

	@Override
	public String toString() {
	    return "!dc";
	}
    }

    /**
     * A class that represents a regular expression specified as an expected
     * value.
     */
    static class Regexp implements CompareExpected {
	private final Pattern pattern;

	Regexp(String pattern) {
	    this.pattern = Pattern.compile(convertPattern(pattern));
	    if (DEBUG) {
		System.err.println("Regexp: '" + pattern + "' => '"
			+ this.pattern + "'");
	    }
	}

	@Override
	public boolean compareExpected(Object actual) {
	    boolean result = pattern.matcher(String.valueOf(actual)).matches();
	    if (DEBUG) {
		System.err.println("Regexp.compareExpected pattern='" + pattern
			+ "', actual='" + actual + "' => '" + result + "'");
	    }
	    return result;
	}

	@Override
	public String toString() {
	    return "!re '" + pattern + "'";
	}
    }

    /**
     * Convert a pattern from the input format, with {N} for captured groups,
     * to the \N format used by Java regexps.
     */
    static String convertPattern(String pattern) {
	if (pattern.indexOf("{") == -1) {
	    return pattern;
	} else {
	    /*
	     * Replace {N} with \N.  To make sure that the '{' is not escaped,
	     * require that the brace is either at the beginning of the input,
	     * right after the last match, that the previous character is not a
	     * backslash, or that the previous two characters are backslashes,
	     * for an escaped backslash.  Note that backslashes need to be
	     * doubled to get them into the string, and then doubled again for
	     * regexp processing to treat them as literals.
	     */
	    return pattern.replaceAll(
		    "(\\A|\\G|[^\\\\]|\\\\\\\\)[{]([0-9]+)[}]", "$1\\\\$2");
	}
    }

    /**
     * A SnakeYAML constructor that converts dc tags to DontCare.INSTANCE and
     * re tags to Regexp instances.
     */
    private static class RegisterTags extends SafeConstructor {
	RegisterTags() {
	    yamlConstructors.put(new Tag("!dc"), new ConstructDontCare());
	    yamlConstructors.put(new Tag("!re"), new ConstructRegexp());
	    yamlConstructors.put(new Tag("!select-engine"),
		    new ConstructSelectEngine());
	    yamlConstructors.put(new Tag("!date"), new ConstructSystemDate());
	    yamlConstructors.put(new Tag("!time"), new ConstructSystemTime());
	    yamlConstructors.put(new Tag("!datetime"),
		    new ConstructSystemDateTime());
	}

	private static class ConstructDontCare extends AbstractConstruct {
	    @Override
	    public Object construct(Node node) {
		return DontCare.INSTANCE;
	    }
	}

	private static class ConstructRegexp extends AbstractConstruct {
	    @Override
	    public Object construct(Node node) {
		if (!(node instanceof ScalarNode)) {
		    fail("The value of the regular expression (!re) tag must"
			    + " be a scalar");
		}
		return new Regexp(((ScalarNode) node).getValue());
	    }
	}

	private class ConstructSelectEngine extends AbstractConstruct {
	    @Override
	    public Object construct(Node node) {
		if (!(node instanceof MappingNode)) {
		    fail("The value of the !select-engine tag must be a map"
			    + "\nGot: " + node);
		}
		String matchingKey = null;
		Object result = null;
		for (NodeTuple tuple : ((MappingNode) node).getValue()) {
		    Node keyNode = tuple.getKeyNode();
		    if (!(keyNode instanceof ScalarNode)) {
			fail("The key in a !select-engine map must be a scalar"
				+ "\nGot: " + constructObject(keyNode));
		    }
		    String key = ((ScalarNode) keyNode).getValue();
		    if (NEWTYPES_ENGINE.equals(key) && Types3Switch.ON) {
		        matchingKey = key;
		        result = constructObject(tuple.getValueNode());
		    }
		    else if (IT_ENGINE.equals(key)
			    || (matchingKey == null && ALL_ENGINE.equals(key))) {
			matchingKey = key;
			result = constructObject(tuple.getValueNode());
		    }
		}
		if (matchingKey != null) {
		    if (DEBUG) {
			System.err.println("Select engine: '" + matchingKey
				+ "' => '" + result + "'");
		    }
		    return result;
		} else {
		    if (DEBUG) {
			System.err.println("Select engine: no match");
		    }
		    return null;
		}
	    }
	}

	private static class ConstructSystemDate extends AbstractConstruct {
	    @Override
	    public Object construct(Node node) {
		Date today = new Date();
		return DEFAULT_DATE_FORMAT.format(today);
	    }

	}

	private static class ConstructSystemTime extends AbstractConstruct {
	    @Override
	    public Object construct(Node node) {
		return new TimeChecker();
	    }
	}

	private static class ConstructSystemDateTime extends AbstractConstruct {
	    @Override
	    public Object construct(Node node) {
		return new DateTimeChecker();
	    }
	}

    }

    /**
     * A class that compares a time value allowing a 1 minute window 
     */
    static class TimeChecker implements CompareExpected {

	private static final int MINUTES_IN_SECONDS = 60;
	private static final int HOURS_IN_MINUTES = 60;

	@Override
	public boolean compareExpected(Object actual) {
	    String[] timeAsString = String.valueOf(actual).split(":");
	    Calendar localCalendar = Calendar.getInstance();
            localCalendar.setTimeInMillis(System.currentTimeMillis());

	    long localTimeInSeconds = localCalendar.get(Calendar.HOUR_OF_DAY)
		    * MINUTES_IN_SECONDS * HOURS_IN_MINUTES;
	    localTimeInSeconds += localCalendar.get(Calendar.MINUTE)
		    * MINUTES_IN_SECONDS;
	    localTimeInSeconds += localCalendar.get(Calendar.SECOND);
	    long resultTime = Integer.parseInt(timeAsString[0])
		    * MINUTES_IN_SECONDS * HOURS_IN_MINUTES;
	    resultTime += Integer.parseInt(timeAsString[1])
		    * MINUTES_IN_SECONDS;
	    resultTime += Integer.parseInt(timeAsString[2]);
	    boolean results = Math.abs(resultTime - localTimeInSeconds) < (1 * MINUTES_IN_SECONDS);
	    return results;
	}

    }

    /**
     *  A class that compares a datetime value allowing a 1 minute window
     */
    static class DateTimeChecker implements CompareExpected {

	private static final int MINUTES_IN_SECONDS = 60;
	private static final int SECONDS_IN_MILLISECONDS = 1000;

	@Override
	public boolean compareExpected(Object actual) {
            Date now = new Date();
            Date date;
	    try {
                date = DEFAULT_DATETIME_FORMAT.parse(
                    String.valueOf(actual) + " UTC");
	    } catch (ParseException e) {
		fail(e.getMessage());
                throw new AssertionError();
	    }
            long testResult = date.getTime() - now.getTime();
	    boolean results = Math.abs(testResult) < (1 * MINUTES_IN_SECONDS * SECONDS_IN_MILLISECONDS);
	    return results;
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

    /** An assertion error that includes context information. */
    private class ContextAssertionError extends AssertionError {
	ContextAssertionError(String failedStatement, String message) {
	    super(context(failedStatement) + message);
	}

	ContextAssertionError(String failedStatement, String message, Throwable cause) {
	    super(context(failedStatement) + message);
	    initCause(cause);
	}
    }

    private String context(String failedStatement) {
	StringBuilder context = new StringBuilder();
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
		context.append(" (").append(commandName);
        if (failedStatement != null)
            context.append(": <").append(failedStatement).append('>');
        context.append(')');
	    }
	}
	if (context.length() != 0) {
	    context.append(": ");
	}
	return context.toString();
    }
    
    private void jmxCommand(Object value, List<Object> sequence)
            throws SQLException {
        if (value != null) {
            new JMXCommand(value, sequence).execute();
        }
    }
    
    private class JMXCommand extends AbstractStatementCommand {

        ArrayList<Object> output = null;
        ArrayList<Object> split_output = null;
        String objectName = null;
        Object[] params = null;
        String method = null;
        String set = null;
        String get = null;
        
        public JMXCommand(Object value, List<Object> sequence) {
            super(string(value, "JMX value"));
            if (value != null & String.valueOf(value).trim().length() > 1) {
                objectName = String.valueOf(value).trim();
            } else {
                fail("Must provide an Object name");
            }

            for (int i = 1; i < sequence.size(); i++) {
                Entry<Object, Object> map = onlyEntry(sequence.get(i),
                "JMX attribute");
                String attribute = string(map.getKey(), "JMX attribute name");
                Object attributeValue = map.getValue();
                if ("params".equals(attribute)) {
                    parseParams(attributeValue);
                } else if ("method".equals(attribute)) {
                    method = String.valueOf(attributeValue).trim();
                } else if ("set".equals(attribute)) {
                    set = String.valueOf(attributeValue).trim();
                } else if ("get".equals(attribute)) {
                    get = String.valueOf(attributeValue).trim();
                } else if ("output".equals(attribute)) {
                    parseOutput(attributeValue);
                } else if ("split_result".equals(attribute)) {
                    parseSplit(attributeValue);
                } else {
                    fail("The '" + attribute + "' attribute name was not"
                            + " recognized");
                }
            }

        }

        private void parseParams(Object value) {
            if (value == null) {
                return;
            }
            assertNull("The params attribute must not appear more than once",
                    params);
            ArrayList<Object> list = new ArrayList<Object>(
                    stringSequence(value, "params value"));
            params = list.toArray(new Object[list.size()]);
        }

        private void parseSplit (Object value) {
            if (value == null) {
                split_output = null;
                return;
            }
            assertNull ("The split_result attribute must not appear more than once", split_output);
            assertNull ("The output and split_result attributes can not appear together", output);
            List<List<Object>> rows = rows(value, "output split value");
            split_output = new ArrayList<Object>(rows.size());
            for (List<?> row : rows) {
                assertEquals("number of entries in row "+ row, 1, row.size());
                split_output.add(row.get(0));
            }
        }
        
        private void parseOutput(Object value) {
            if (value == null) {
                output = null;
                return;
            }
            assertNull("The output attribute must not appear more than once", output);
            assertNull("The split_result and output attributes can not appear together", split_output);
            
            List<List<Object>> rows = rows(value, "output value");
            output = new ArrayList<Object>(rows.size());
            for (List<?> row : rows) {
                assertEquals("number of entries in row " + row, 1, row.size());
                output.add(row.get(0));
            }
        }

        public void execute() {
            JMXInterpreter conn = new JMXInterpreter(DEBUG);
            Object result = null;
            try {
                if (method != null) {
                    result = conn.makeBeanCall("localhost", 8082, objectName,
                            method, params, "method");
//                    if (DEBUG) {
//                        System.out.println("makeBeanCall(localhost, 8082, "+objectName+", "+method+")");
//                        System.out.println(result);
//                    }
                }
                if (set != null) {
                    conn.makeBeanCall("localhost", 8082, objectName, set, params, "set");
//                    if (DEBUG) {
//                        System.out.println("makeBeanCall(localhost, 8082, "+objectName+", "+set+")");
//                    }
                }
                if (get != null) {
                    result = conn.makeBeanCall("localhost", 8082, objectName, get, params, "get");
//                    if (DEBUG) {
//                        System.out.println("makeBeanCall(localhost, 8082, "+objectName+", "+get+")");
//                        System.out.println(result);
//                    }

                }
                
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error: " + e.getMessage());
                fail("Error: " + e.getMessage());
            }
            
            if (split_output != null) {
                if (result == null)
                    fail("found null; expected: " + split_output);
                List<Object> actuals = new ArrayList<Object>(Arrays.asList(result.toString().split("\\n")));
                int highestCommon = Math.min(actuals.size(), split_output.size());
                for (int i = 0; i < highestCommon; ++i) {
                    if (split_output.get(i) == DontCare.INSTANCE)
                        actuals.set(i, DontCare.INSTANCE);
                }
                assertCollectionEquals(split_output, actuals);
            } else if (output != null) {
                if (result == null) 
                    fail ("found null; expected: " + output);
                List<Object> actuals = new ArrayList<Object>(Arrays.asList(result.toString()));
                int highestCommon = Math.min(actuals.size(), output.size());
                for (int i = 0; i < highestCommon; i++) {
                    if (output.get(i) == DontCare.INSTANCE)
                        actuals.set(i, DontCare.INSTANCE);
                }
                assertCollectionEquals(output, actuals);
            }
        }

    }
}
