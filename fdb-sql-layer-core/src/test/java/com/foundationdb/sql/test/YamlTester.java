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

package com.foundationdb.sql.test;

import static com.foundationdb.util.AssertUtils.assertCollectionEquals;
import static com.foundationdb.util.FileTestUtils.printClickableFile;
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

import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.regex.Pattern;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.util.Strings;
import org.junit.ComparisonFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;


/** A utility for testing SQL access over a Postgres server connection based on the contents of a YAML file. */
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
     - output_already_ordered: [[<output value>, ...], ...]
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
   - output: does a sort on the expected and actual during comparison
   - Warnings include statement warnings followed by result set warnings for
     each output row
   - The warning message is optional
   
   - JMX: <objectName>   (i.e com.foundationdb:type=IndexStatistics)
   ** Only one allowed of the following three (3) per command set
   - set: <set method>
   - method: <method>
   - get: <get method>
   
   - params: [<parameter value>, ...]
   - output: [[<output value>, ...], ...]
*/
public class YamlTester
{
    private static final Logger LOG = LoggerFactory.getLogger(YamlTester.class);
    
    /** Matches all engines. */
    private static final String ALL_ENGINE = "all";
    /** Matches the IT engine. */
    private static final String IT_ENGINE = "it";
    /** Matches the Random Cost engine. */
    private static final String RAND_COST_ENGINE = "random-cost";

    /**
     * Compare two Lists into a consistent, though not necessarily ordered, order.
     * Actual contents are expected to be compared more strictly after using this.
     */
    private static final Comparator<List<?>> SIMPLE_LIST_COMPARATOR = new Comparator<List<?>>()
    {
        public int compare(List<?> x, List<?> y) {
            assertEquals("list sizes", x.size(), y.size());
            for(int i = 0; i < x.size(); i++) {
                Object xObj = x.get(i);
                Object yObj = y.get(i);
                // Regex, etc are not safe sortable with respect to the the final order
                assertFalse("CompareExpected in unsorted -output: " + xObj, xObj instanceof CompareExpected);
                assertFalse("CompareExpected in unsorted -output: " + yObj, yObj instanceof CompareExpected);
                String xString = objectToString(xObj);
                String yString = objectToString(yObj);
                int cmp = xString.compareTo(yString);
                if(cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    };

    private final URL sourceURL;
    private final Reader in;
    private final Connection connection;
    private final Deque<URL> includeStack = new ArrayDeque<>();
    private final boolean randomCost;
    private int commandNumber = 0;
    private String commandName = null;
    private boolean suppressed = false;
    private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat DEFAULT_DATETIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");
    private static final int DEFAULT_RETRY_COUNT = 5;
    private int lineNumber = 1;

    public YamlTester(Reader in, Connection connection) {
        this(null, in, connection, false);
    }

    public YamlTester(URL sourceURL, Reader in, Connection connection,
                      boolean randomCost) {
        this.sourceURL = sourceURL;
        this.in = in;
        this.connection = connection;
        this.randomCost = randomCost;
    }

    /** Test the input specified in the constructor. */
    public void test() {
        try {
            if (sourceURL != null && sourceFilename().contains("current-date-time"))
                System.out.println("Starting " + sourceFilename() + " @ " + DEFAULT_DATETIME_FORMAT.format(new Date()));
            test(in);
            if (sourceURL != null sourceFilename().contains("current-date-time"))
                System.out.println("Done " + sourceFilename() + " @ " + DEFAULT_DATETIME_FORMAT.format(new Date()));
        } catch (Throwable e) {
            if (sourceURL != null) {
                System.err.println("Failed Yaml test (note: line number points to start of document)");
                String filename = sourceFilename();
                printClickableFile(filename.substring(0, filename.length() - 5), "yaml", lineNumber);
            }
            throw e;
        }
    }

    private void test(Reader in) {
        List<?> sequence = null;
        try {
            Yaml yaml = new Yaml(new RegisterTags());
            for(Object yamlObject : yaml.loadAll(in)) {
                RegisterTags.LinedObject linedDocument = (RegisterTags.LinedObject) yamlObject;
                Object document = linedDocument.getObject();
                lineNumber = linedDocument.getStartMark().getLine();
                ++commandNumber;
                commandName = null;
                sequence = nonEmptySequence(document, "command document");
                Entry<?,?> firstEntry = firstEntry(sequence.get(0), "first element of the document");
                commandName = string(firstEntry.getKey(), "command name");
                Object value = firstEntry.getValue();
                if("Include".equals(commandName)) {
                    includeCommand(value, sequence);
                } else if("Properties".equals(commandName)) {
                    propertiesCommand(value, sequence);
                } else if("CreateTable".equals(commandName)) {
                    createTableCommand(value, sequence);
                } else if("DropTable".equals(commandName)) {
                    dropTableCommand(value, sequence);
                } else if("Statement".equals(commandName)) {
                    statementCommand(value, sequence);
                } else if("Message".equals(commandName)) {
                    messageCommand(value);
                } else if("JMX".equals(commandName)) {
                    jmxCommand(value, sequence);
                } else if ("UseContext".equals(commandName)) {
                    useContextCommand(value, sequence);
                } else if("Newtypes".equals(commandName)) {
                    fail("No longer supported Newtypes command");
                } else {
                    fail("Unknown command: " + commandName);
                }
                if(suppressed) {
                    LOG.debug("Test suppressed: {}", sourceURL);
                    break;
                }
            }
            if(commandNumber == 0) {
                fail("Test file must not be empty");
            }
        } catch(ContextAssertionError | FullOutputAssertionError e) {
            throw e;
        } catch(Throwable e) {
            // Add context
            throw new ContextAssertionError(String.valueOf(sequence), e.toString(), e);
        }
    }

    private void includeCommand(Object value, List<?> sequence) {
        if(value == null) {
            return;
        }
        String includeValue = string(value, "Include value");
        if(sequence.size() > 1) {
            throw new ContextAssertionError(
                includeValue, "The Include command does not support attributes" + "\nFound: " + sequence.get(1)
            );
        }
        URL context = sourceURL;
        if (!includeStack.isEmpty()) {
            context = includeStack.peek();
        }
        URL include;
        try {
            include = new URL(context, includeValue);
        }
        catch (MalformedURLException ex) {
            throw new ContextAssertionError(includeValue, "Bad include value " + includeValue + ": " + ex, ex);
        }
        try(Reader in = new InputStreamReader(include.openStream(), "UTF-8")) {
            int originalCommandNumber = commandNumber;
            commandNumber = 0;
            String originalCommandName = commandName;
            commandName = null;
            try {
                includeStack.push(include);
                test(in);
            } finally {
                includeStack.pop();
                commandNumber = originalCommandNumber;
                commandName = originalCommandName;
            }
        } catch(IOException e) {
            throw new ContextAssertionError(includeValue, "Problem accessing include file " + include + ": " + e, e);
        }
    }

    private void messageCommand(Object value) {
        String message = string(value, "Message");
        LOG.info("FTS Message: {}", message);
    }

    private void propertiesCommand(Object value, List<?> sequence) {
        String engine = string(value, "Properties framework engine");
        if(ALL_ENGINE.equals(engine) || IT_ENGINE.equals(engine)
                || (randomCost && RAND_COST_ENGINE.equals(engine))) {
            for(Object elem : sequence) {
                Entry<?,?> entry = onlyEntry(elem, "Properties entry");
                if("suppressed".equals(entry.getKey())) {
                    suppressed = bool(entry.getValue(), "suppressed value");
                }
            }
        }
    }

    /** Implements common behavior of commands that execute statements. */
    private abstract class AbstractStatementCommand
    {
        final String statement;
        boolean errorSpecified;
        Object errorCode;
        Object errorMessage;
        boolean doSortOutput;
        int retryCount = -1;
        int retriesPerformed = 0;

        /** Handle a statement with the specified statement text. */
        AbstractStatementCommand(String statement) {
            this.statement = statement;
        }

        /** Parse an error attribute with the specified value. */
        void parseError(Object value) {
            if(value == null) {
                return;
            }
            assertFalse(
                "The error attribute must not appear more than once", errorSpecified
            );
            errorSpecified = true;
            List<?> errorInfo = nonEmptyScalarSequence(value, "error value");
            errorCode = scalar(errorInfo.get(0), "error code");
            if(errorInfo.size() > 1) {
                errorMessage = scalar(errorInfo.get(1), "error message");
                assertTrue("The error attribute can have at most two elements", errorInfo.size() < 3);
            }
        }

        /**
         * Check the specified exception against the error attribute specified earlier, if any.
         *
         * @return <code>true</code> if the original statement should be retried, <code>false</code> otherwise
         */
        boolean checkFailure(SQLException sqlException) {
            LOG.debug("Generated error code: {}", sqlException.getSQLState(), sqlException);
            if(!errorSpecified) {
                // Retry rollback automatically if command did not give an explicit request
                if(retryCount == -1) {
                    try {
                        if(ErrorCode.valueOfCode(sqlException.getSQLState()).isRollbackClass()) {
                            retryCount = DEFAULT_RETRY_COUNT;
                        }
                    } catch(IllegalArgumentException e) {
                        // Ignore, wasn't an ErrorCode
                    }
                }
                if(retriesPerformed < retryCount) {
                    LOG.debug("Unexpected error, retrying statement.");
                    ++retriesPerformed;
                    try {
                        // Delay a little to try and avoid another immediate conflict
                        Thread.sleep(100 * retriesPerformed);
                    } catch(InterruptedException e) {
                        // Ignore
                    }
                    return true;
                }
                throw new ContextAssertionError(statement, "Unexpected statement execution failure", sqlException);
            }
            checkExpected("error code", errorCode, sqlException.getSQLState());
            if(errorMessage != null) {
                checkExpected("error message", errorMessage, sqlException.getMessage());
            }
            return false;
        }


    }

    /** Represents an SQL warning. */
    private static class Warning implements CompareExpected
    {
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
            if(message != null) {
                sb.append(", '").append(message).append("'");
            }
            sb.append("]");
            return sb.toString();
        }

        public boolean compareExpected(Object actual) {
            if(!(actual instanceof Warning)) {
                return false;
            }
            Warning warning = (Warning)actual;
            // Require the codes to match
            if(!expected(code, warning.code)) {
                return false;
            }
            // Only require the message to match if the pattern -- this object -- specifies a message
            return message == null || expected(message, warning.message);
        }
    }

    private void createTableCommand(Object value, List<?> sequence) throws SQLException {
        new CreateTableCommand(value, sequence).execute();
    }

    private class CreateTableCommand extends AbstractStatementCommand
    {
        private Object warningsCount;
        private List<Warning> warnings;

        CreateTableCommand(Object value, List<?> sequence) {
            super("CREATE TABLE " + string(value, "CreateTable argument"));
            for(int i = 1; i < sequence.size(); i++) {
                Entry<?,?> map = onlyEntry(sequence.get(i), "CreateTable attribute");
                String attribute = string(map.getKey(), "CreateTable attribute name");
                Object attributeValue = map.getValue();
                if("error".equals(attribute)) {
                    parseError(attributeValue);
                } else if("warnings_count".equals(attribute)) {
                    warningsCount = parseWarningsCount(attributeValue, warningsCount);
                } else if("warnings".equals(attribute)) {
                    warnings = parseWarnings(attributeValue, warnings);
                } else {
                    fail("The '" + attribute + "' attribute name was not" + " recognized");
                }

                if(warnings != null && warningsCount != null && !expected(warningsCount, warnings.size())) {
                    fail("Warnings count " + warningsCount + " does not match " + warnings.size() + ", which is the number of warnings");
                }
            }
        }

        void execute() throws SQLException {
            Statement stmt = connection.createStatement();
            LOG.debug("Executing statement: {}", statement);
            while(true) {
                try {
                    stmt.execute(statement);
                    LOG.debug("Statement executed successfully");
                    break;
                } catch(SQLException e) {
                    LOG.debug("Generated error code: {}", e.getSQLState(), e);
                    if(checkFailure(e)) {
                        continue;
                    }
                    return;
                }
            }
            checkSuccess(stmt, errorSpecified, warnings, warningsCount);
        }
    }


    private void dropTableCommand(Object value, List<?> sequence) throws SQLException {
        new DropTableCommand(value, sequence).execute();
    }


    private class DropTableCommand extends AbstractStatementCommand
    {

        private Object warningsCount;
        private List<Warning> warnings;

        DropTableCommand(Object value, List<?> sequence) {
            super("DROP TABLE " + string(value, "DropTable argument"));
            for(int i = 1; i < sequence.size(); i++) {
                Entry<?,?> map = onlyEntry(sequence.get(i), "DropTable attribute");
                String attribute = string(map.getKey(), "CreateTable attribute name");
                Object attributeValue = map.getValue();
                if("error".equals(attribute)) {
                    parseError(attributeValue);
                } else if("warnings_count".equals(attribute)) {
                    warningsCount = parseWarningsCount(attributeValue, warningsCount);
                } else if("warnings".equals(attribute)) {
                    warnings = parseWarnings(attributeValue, warnings);
                } else {
                    fail("The '" + attribute + "' attribute name was not" + " recognized");
                }

                if(warnings != null && warningsCount != null && !expected(warningsCount, warnings.size())) {
                    fail("Warnings count " + warningsCount + " does not match " + warnings.size() + ", which is the number of warnings");
                }
            }
        }

        void execute() throws SQLException {
            Statement stmt = connection.createStatement();
            LOG.debug("Executing statement: {}", statement);
            while(true) {
                try {
                    stmt.execute(statement);
                    LOG.debug("Statement executed successfully");
                    break;
                } catch(SQLException e) {
                    LOG.debug("Generated error code: {}", e.getSQLState(), e);
                    if(checkFailure(e)) {
                        continue;
                    }
                    return;
                }
            }
            checkSuccess(stmt, errorSpecified, warnings, warningsCount);
        }
    }

    private void useContextCommand(Object value, List<?> sequence) throws SQLException {
        new UseContextCommand(value, sequence).execute();
    }

    private class UseContextCommand extends AbstractStatementCommand
    {
        UseContextCommand (Object context, List<?>sequence) {
            super("Use Context: " + string(context, "UseContext Argument"));
        }
        
        void execute() throws SQLException {
        }
    }

    
    private void statementCommand(Object value, List<?> sequence) throws SQLException {
        assertNotNull("Statement value cannot be null (e.g. null, empty, no matching select-engine)", value);
        new StatementCommand(string(value, "Statement value"), sequence).execute();
    }

    private class StatementCommand extends AbstractStatementCommand
    {
        private List<List<?>> params;
        private List<Integer> paramTypes;
        private List<List<?>> output;
        private int rowCount = -1;
        private List<String> outputTypes;
        private Object explain;
        private Object warningsCount;
        private List<Warning> warnings;

        /** The 1-based index of the row of parameters being used for the current parameterized statement execution. */
        private int paramsRow = 1;

        /** The 0-based index of the row of the output being compared with the statement output. */
        private int outputRow = 0;

        StatementCommand(String statement, List<?> sequence) {
            super(statement);
            for(int i = 1; i < sequence.size(); i++) {
                Entry<?,?> map = onlyEntry(
                    sequence.get(i), "Statement attribute"
                );
                String attribute = string(
                    map.getKey(), "Statement attribute name"
                );
                Object attributeValue = map.getValue();
                if("params".equals(attribute)) {
                    parseParams(attributeValue);
                } else if("param_types".equals(attribute)) {
                    parseParamTypes(attributeValue);
                } else if("output_already_ordered".equals(attribute)) {
                    this.doSortOutput = false;
                    parseOutput(attributeValue);
                } else if("output".equals(attribute)) {
                    this.doSortOutput = true;
                    parseOutput(attributeValue);
                } else if("row_count".equals(attribute)) {
                    parseRowCount(attributeValue);
                } else if("retry_count".equals(attribute)) {
                    parseRetryCount(attributeValue);
                } else if("output_types".equals(attribute)) {
                    parseOutputTypes(attributeValue);
                } else if("error".equals(attribute)) {
                    parseError(attributeValue);
                } else if("explain".equals(attribute)) {
                    parseExplain(attributeValue);
                } else if("warnings_count".equals(attribute)) {
                    warningsCount = parseWarningsCount(attributeValue, warningsCount);
                } else if("warnings".equals(attribute)) {
                    warnings = parseWarnings(attributeValue, warnings);
                } else {
                    fail("The '" + attribute + "' attribute name was not" + " recognized");
                }
            }
            if(paramTypes != null) {
                if(params == null) {
                    fail("Cannot specify the param_types attribute without" + " params attribute");
                } else {
                    assertEquals(
                        "The params_types attribute must be the same" + " length as the row length of the params" + " attribute:",
                        params.get(0).size(),
                        paramTypes.size()
                    );
                }
            }
            if(rowCount != -1) {
                if(output != null) {
                    assertEquals(
                        "The row_count attribute must be the same" + " as the length of the rows in the output" + " attribute:",
                        output.size(),
                        rowCount
                    );
                } else if(outputTypes != null) {
                    assertEquals(
                        "The row_count attribute must be the same" + " as the length of the output_types" + " attribute:",
                        outputTypes.size(),
                        rowCount
                    );
                }
            }
            if(outputTypes != null) {
                if(output != null) {
                    assertEquals(
                        "The output_types attribute must be the same" + " length as the length of the rows in the" + " output attribute:",
                        output.get(0).size(),
                        outputTypes.size()
                    );
                }
            }
            if(errorSpecified && output != null) {
                fail("Cannot specify both error and output attributes");
            }
            if(errorSpecified && rowCount != -1) {
                fail("Cannot specify both error and row_count attributes");
            }
            if(warnings != null &&
                warningsCount != null &&
                !expected(warningsCount, warnings.size())) {
                fail("Warnings count " + warningsCount + " does not match " + warnings.size() + ", which is the number of warnings");
            }
        }

        private void parseParams(Object value) {
            if(value == null) {
                return;
            }
            assertNull("The params attribute must not appear more than once", params);
            params = rows(value, "params value");
            // TODO: -output needs to (optionally?) be another list nesting deep for clarity with multiple params
            assertEquals("params size", 1, params.size());
        }

        private void parseParamTypes(Object value) {
            if(value == null) {
                return;
            }
            assertNull("The param_types attribute must not appear more than once", paramTypes);
            List<String> paramTypeNames = nonEmptyStringSequence(
                value, "param_types value"
            );
            paramTypes = new ArrayList<>(paramTypeNames.size());
            for(String typeName : paramTypeNames) {
                Integer typeNumber = getTypeNumber(typeName);
                assertNotNull("Unknown type name in param_types: " + typeName, typeNumber);
                paramTypes.add(typeNumber);
            }
        }

        private void parseOutput(Object value) {
            if(value == null) {
                return;
            }
            assertNull("The output attribute must not appear more than once", output);
            output = rows(value, "output value");
        }

        private void parseRowCount(Object value) {
            if(value == null) {
                return;
            }
            assertTrue("The row_count attribute must not appear more than once", rowCount == -1);
            rowCount = integer(value, "row_count value");
            assertTrue("The row_count value must not be negative", rowCount >= 0);
        }

        private void parseRetryCount(Object value) {
            assertTrue("The retry_count attribute must not appear more than once", retryCount == -1);
            int count = DEFAULT_RETRY_COUNT;
            if(value != null) {
                count = integer(value, "retry_count value");
                assertTrue("The retry_count value must not be negative", count >= 0);
            }
            retryCount = count;
        }

        private void parseOutputTypes(Object value) {
            if(value == null) {
                return;
            }
            assertNull("The output_types attribute must not appear more than once", outputTypes);
            outputTypes = nonEmptyStringSequence(value, "output_types value");
        }

        private void parseExplain(Object value) {
            if(value == null) {
                return;
            }
            assertNull("The explain attribute must not appear more than once", explain);
            explain = scalar(value, "explain value");
        }

        void execute() throws SQLException {
            if(explain != null) {
                checkExplain();
            }
            boolean done = false;
            while(!done) {
                if(params == null) {
                    try(Statement stmt = connection.createStatement()) {
                        LOG.debug("Executing statement: {}", statement);
                        try {
                            stmt.execute(statement);
                        } catch(SQLException e) {
                            if(checkFailure(e)) {
                                continue;
                            }
                            return;
                        }
                        checkSuccess(stmt, doSortOutput);
                    }
                } else {
                    try(PreparedStatement stmt = connection.prepareStatement(statement)) {
                        int numParams = params.get(0).size();
                        for(List<?> paramsList : params) {
                            if(params.size() > 1) {
                                commandName = "Statement, params list " + paramsRow;
                            }
                            for(int i = 0; i < numParams; i++) {
                                Object param = paramsList.get(i);
                                if(paramTypes != null) {
                                    stmt.setObject(i + 1, param, paramTypes.get(i));
                                } else {
                                    stmt.setObject(i + 1, param);
                                }
                            }
                            LOG.debug("Executing statement: {}   with Parameters: {}", statement, paramsList);
                            try {
                                stmt.execute();
                            } catch(SQLException e) {
                                if(checkFailure(e)) {
                                    continue;
                                }
                                continue;
                            }
                            checkSuccess(stmt, doSortOutput);
                            paramsRow++;
                        }
                        commandName = "Statement";
                    }
                }
                done = true;
            }
        }

        private void checkExplain() throws SQLException {
            try(Statement stmt = connection.createStatement()) {
                stmt.execute("EXPLAIN " + statement);
                ResultSet rs = stmt.getResultSet();
                StringBuilder sb = new StringBuilder();
                int numColumns = rs.getMetaData().getColumnCount();
                while(rs.next()) {
                    for(int i = 1; i <= numColumns; i++) {
                        if(i != 1) {
                            sb.append(", ");
                        }
                        sb.append(rs.getString(i));
                    }
                    sb.append('\n');
                }
                String got = sb.toString().trim();
                checkExpected("explain output", explain, got);
            }
        }

        private void checkSuccess(Statement stmt, boolean doSortOutput) throws SQLException {
            assertFalse("Statement execution succeeded, but was expected" + " to generate an error", errorSpecified);
            ResultSet rs = stmt.getResultSet();
            if(rs == null) {
                assertNull("Query did not produce results output", output);
                assertNull("Query did not produce results, so output_types" + " are not supported", outputTypes);
                if(rowCount != -1) {
                    int updateCount = stmt.getUpdateCount();
                    assertFalse("Query did not produce an update count", updateCount == -1);
                    outputRow += updateCount;
                    checkRowCount(rowCount, false);
                }
                List<Warning> reportedWarnings = new ArrayList<>();
                collectWarnings(stmt.getWarnings(), reportedWarnings);
                checkWarnings(reportedWarnings);
            } else {
                checkResults(rs, doSortOutput);
                assertFalse("Multiple result sets not supported", stmt.getMoreResults());
            }
        }

        /**
         * Add the warning message from the specified warning, as well as any additional warnings linked via the
         * getNextWarning method, to the list of messages.
         */
        private void collectWarnings(SQLWarning warning, List<Warning> messages) {
            while(warning != null) {
                messages.add(new Warning(warning.getSQLState(), warning.getMessage()));
                warning = warning.getNextWarning();
            }
        }

        private void checkWarnings(List<Warning> reportedWarnings) {
            if(!reportedWarnings.isEmpty()) {
                LOG.debug("Statement warnings: {}", reportedWarnings);
            }
            if(warningsCount != null) {
                checkExpected("warnings count", warningsCount, reportedWarnings.size());
            }
            if(warnings == null) {
                return;
            }
            if(reportedWarnings.isEmpty()) {
                if(!warnings.isEmpty()) {
                    fail("No warnings were reported, but expected warnings: " + warnings);
                }
            } else {
                if(warnings.isEmpty()) {
                    fail("Warnings were reported but none were expected: " + warnings);
                }
                checkExpectedList("Warnings", warnings, reportedWarnings);
            }
        }

        /**
         * Check if the number of rows of output seen, as measured by the outputRow field, is incorrect given the expected
         * number of rows.
         *
         * @param expected the number of rows expected
         * @param more     whether there are more result rows in the current result set
         */
        private void checkRowCount(int expected, boolean more) {
            int got = outputRow;
            if(more) {
                got++;
            }
            if(got > expected) {
                throw new ContextAssertionError(
                    statement, "Too many output rows:" + "\nExpected: " + expected + "\n     got: " + got
                );
            } else if(!more && (params == null || paramsRow == params.size()) && (got < expected)) {
                throw new ContextAssertionError(
                    statement, "Too few output rows:" + "\nExpected: " + expected + "\n     got: " + got
                );
            }
        }

        private void checkResults(ResultSet rs, boolean doSortOutput) throws SQLException {
            if(outputTypes != null && outputRow == 0) {
                checkOutputTypes(rs);
            }
            LOG.debug("Statement output:");
            List<Warning> reportedWarnings = new ArrayList<>();
            if(output != null) {
                ResultSetMetaData metaData = rs.getMetaData();
                int numColumns = metaData.getColumnCount();
                List<List<?>> resultsList = new ArrayList<>();
                Statement stmt = rs.getStatement();
                assert stmt != null;
                collectWarnings(stmt.getWarnings(), reportedWarnings);
                while (rs.next()) {
                    ArrayList<Object> actualRow = new ArrayList<>(metaData.getColumnCount());
                    resultsList.add(actualRow);
                    for (int i=0; i<metaData.getColumnCount(); i++) {
                        actualRow.add(rs.getObject(i+1));
                    }
                }
                try {
                    assertEquals("Unexpected number of columns in output:", output.get(0).size(), numColumns);
                    if (doSortOutput) {
                        Collections.sort(output, SIMPLE_LIST_COMPARATOR);
                        Collections.sort(resultsList, SIMPLE_LIST_COMPARATOR);
                    }
                    for (int i=0; outputRow < output.size() && i < resultsList.size(); outputRow++, i++) {
                        List<?> row = output.get(outputRow);
                        List<?> resultsRow = resultsList.get(i);
                        if (!rowsEqual(row, resultsRow)) {
                            throw new ContextAssertionError(
                                    statement,
                                    "Unexpected output in row " + (outputRow + 1) + ":" +
                                            " Expected: " + arrayString(row) + " got: " + arrayString(resultsRow)
                            );
                        }
                    }
                    assertEquals("Unexpected number of rows", output.size(), resultsList.size());
                } catch (ContextAssertionError e) {
                    throw new FullOutputAssertionError(output, resultsList, e);
                } catch (AssertionError e) {
                    throw new FullOutputAssertionError(output, resultsList, new ContextAssertionError(statement, e));
                }
            } else {
                Statement stmt = rs.getStatement();
                assert stmt != null;
                collectWarnings(stmt.getWarnings(), reportedWarnings);
                while(rs.next()) {
                    outputRow++;
                    collectWarnings(rs.getWarnings(), reportedWarnings);
                }
                if(rowCount != -1) {
                    checkRowCount(rowCount, false);
                }
            }
            checkWarnings(reportedWarnings);
        }

        private boolean rowsEqual(List<?> pattern, List<?> row) {
            int size = pattern.size();
            if(size != row.size()) {
                return false;
            }
            for(int i = 0; i < size; i++) {
                Object patternElem = pattern.get(i);
                Object rowElem = row.get(i);
                if(!expected(patternElem, rowElem)) {
                    return false;
                }
            }
            return true;
        }

        private void checkOutputTypes(ResultSet rs) throws SQLException {
            ResultSetMetaData metaData = rs.getMetaData();
            int numColumns = metaData.getColumnCount();
            assertEquals("Wrong number of output types:", outputTypes.size(), numColumns);
            for(int i = 1; i <= numColumns; i++) {
                int columnType = metaData.getColumnType(i);
                String columnTypeName = metaData.getColumnTypeName(i);
                if(columnTypeName == null) {
                    columnTypeName = "<unknown " + metaData.getColumnTypeName(i) + " (" + columnType + ")>";
                }
                assertEquals("Wrong output type for column " + i + ":", outputTypes.get(i - 1), columnTypeName);
            }
        }
    }

    //----------- static helpers -----------------

    static Object stripWARN(Object msg) {
        if(msg instanceof String) {
            String st = (String)msg;
            if(st.startsWith("WARN:  ")) {
                return st.substring(7);
            }
        }
        return msg;
    }

    static void checkSuccess(Statement stmt,
                             boolean errorSpecified,
                             List<Warning> warnings,
                             Object warningsCount) throws SQLException {
        assertFalse("Statement execution succeeded, but was expected" + " to generate an error", errorSpecified);
        List<Warning> reportedWarnings = new ArrayList<>();
        collectWarnings(stmt.getWarnings(), reportedWarnings);
        checkWarnings(reportedWarnings, warnings, warningsCount);
    }

    static Object parseWarningsCount(Object value, Object warningsCount) {
        if(value == null) {
            return null;
        }
        assertNull("The warnings_count attribute must not appear more than once", warningsCount);
        return scalar(value, "warningsCount");
    }

    static List<Warning> parseWarnings(Object value, List<Warning> warnings) {
        if(value == null) {
            return null;
        }
        assertNull("The warnings attribute must not appear more than once", warnings);

        List<?> list = nonEmptySequence(value, "warnings");
        warnings = new ArrayList<>();
        for(int i = 0; i < list.size(); i++) {
            List<?> element = nonEmptyScalarSequence(list.get(i), "warnings element " + i);
            assertFalse("Warnings element " + i + " is empty", element.isEmpty());
            assertFalse("Warnings element " + i + " has more than two elements", element.size() > 2);
            warnings.add(new Warning(element.get(0), element.size() > 1 ? stripWARN(element.get(1)) : null));
        }
        return warnings;
    }

    private static void collectWarnings(SQLWarning warning, List<Warning> messages) {
        while(warning != null) {
            messages.add(new Warning(warning.getSQLState(), warning.getMessage()));
            warning = warning.getNextWarning();
        }
    }

    private static void checkWarnings(List<Warning> reportedWarnings, List<Warning> warnings, Object warningsCount) {
        if(!reportedWarnings.isEmpty()) {
            LOG.debug("Statement warnings: {}", reportedWarnings);
        }

        if(warningsCount != null) {
            checkExpected("warnings count", warningsCount, reportedWarnings.size());
        }
        if(warnings == null) {
            return;
        }
        if(reportedWarnings.isEmpty()) {
            if(!warnings.isEmpty()) {
                fail("No warnings were reported, but expected warnings: " + warnings);
            }
        } else {
            if(warnings.isEmpty()) {
                fail("Warnings were reported but none were expected: " + warnings);
            }
            checkExpectedList("Warnings", warnings, reportedWarnings);
        }
    }

    static void checkExpectedList(String description, List<?> expected, List<?> actual) {
        int expectedSize = expected.size();
        int actualSize = actual.size();
        if(expectedSize != actualSize) {
            fail(
                description + " should have " + expectedSize +
                    " elements, but has " + actualSize + ":" +
                    "\nExpected: " + expected +
                    "\n     got: " + actual
            );
        }
        for(int i = 0; i < expectedSize; i++) {
            Object expectedElement = expected.get(i);
            Object actualElement = actual.get(i);
            if(!expected(expectedElement, actualElement)) {
                fail(
                    "Incorrect value for element " + i + " of " +
                        description + ":" +
                        "\nExpected: " + expected +
                        "\n     got: " + actual
                );
            }
        }
    }

    static void checkExpected(String description, Object expected, Object actual) {
        if(!expected(expected, actual)) {
            fail("Incorrect " + description + ":" + "\nExpected: " + expected + "\n     got: " + actual);
        }
    }

    static boolean expected(Object expected, Object actual) {
        if(expected instanceof CompareExpected) {
            return ((CompareExpected)expected).compareExpected(actual);
        } else if(expected == null) {
            return actual == null;
        } else {
            String expectedString = objectToString(expected);
            String actualString = objectToString(actual);

            return expectedString.equals(actualString);
        }
    }

    static String arrayString(List<?> array) {
        if(array == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for(int i = 0; i < array.size(); i++) {
            Object elem = array.get(i);
            if(i != 0) {
                sb.append(", ");
            }
            sb.append(objectToString(elem));
        }
        sb.append(']');
        return sb.toString();
    }

    static String objectToString(Object object) {
        if(object == null) {
            return "null";
        } else {
            Class objectClass = object.getClass();
            if(!objectClass.isArray()) {
                return object.toString();
            } else if(objectClass == byte[].class) {
                return Arrays.toString((byte[])object);
            } else if(objectClass == short[].class) {
                return Arrays.toString((short[])object);
            } else if(objectClass == int[].class) {
                return Arrays.toString((int[])object);
            } else if(objectClass == long[].class) {
                return Arrays.toString((long[])object);
            } else if(objectClass == char[].class) {
                return Arrays.toString((char[])object);
            } else if(objectClass == float[].class) {
                return Arrays.toString((float[])object);
            } else if(objectClass == double[].class) {
                return Arrays.toString((double[])object);
            } else if(objectClass == boolean[].class) {
                return Arrays.toString((boolean[])object);
            } else {
                return Arrays.toString((Object[])object);
            }
        }
    }

    static Object scalar(Object object, String desc) {
        assertThat(
            "The " + desc + " must be a scalar", object, not(anyOf(instanceOf(Collection.class), instanceOf(Map.class)))
        );
        return object;
    }

    static String string(Object object, String desc) {
        assertThat("The " + desc + " must be a string", object, instanceOf(String.class));
        return (String)object;
    }

    static int integer(Object object, String desc) {
        assertThat("The " + desc + " must be an integer", object, instanceOf(Integer.class));
        return (Integer)object;
    }

    static boolean bool(Object object, String desc) {
        assertThat("The " + desc + " must be a boolean", object, instanceOf(Boolean.class));
        return (Boolean)object;
    }

    static Map<?,?> map(Object object, String desc) {
        assertThat("The " + desc + " must be a map", object, instanceOf(Map.class));
        return (Map<?,?>)object;
    }

    static Entry<?,?> firstEntry(Object object, String desc) {
        Map<?,?> map = map(object, desc);
        assertFalse("The " + desc + " must not be empty", map.isEmpty());
        return map.entrySet().iterator().next();
    }

    static Entry<?,?> onlyEntry(Object object, String desc) {
        Map<?,?> map = map(object, desc);
        assertEquals("The " + desc + " must contain exactly one entry:", 1, map.size());
        return map.entrySet().iterator().next();
    }

    static List<?> sequence(Object object, String desc) {
        assertThat("The " + desc + " must be a sequence", object, instanceOf(List.class));
        return (List<?>)object;
    }

    static List<?> nonEmptySequence(Object object, String desc) {
        List<?> list = sequence(object, desc);
        assertFalse("The " + desc + " must not be empty", list.isEmpty());
        return list;
    }

    static List<?> scalarSequence(Object object, String desc) {
        List<?> list = sequence(object, desc);
        for(Object elem : list) {
            assertThat(
                "The element of the " + desc + " must be a scalar",
                elem,
                not(anyOf(instanceOf(Collection.class), instanceOf(Map.class)))
            );
        }
        return list;
    }

    static List<?> nonEmptyScalarSequence(Object object, String desc) {
        List<?> list = scalarSequence(object, desc);
        assertFalse("The " + desc + " must not be empty", list.isEmpty());
        return list;
    }

    static List<String> stringSequence(Object object, String desc) {
        List<?> list = sequence(object, desc);
        List<String> strList = new ArrayList<>(list.size());
        for(Object elem : list) {
            assertThat("The element of the " + desc + " must be a string", elem, instanceOf(String.class));
            strList.add((String)elem);
        }
        return strList;
    }

    static List<String> nonEmptyStringSequence(Object object, String desc) {
        List<String> list = stringSequence(object, desc);
        assertFalse("The " + desc + " must not be empty", list.isEmpty());
        return list;
    }

    static List<List<?>> rows(Object object, String desc) {
        List<?> list = nonEmptySequence(object, desc);
        List<List<?>> rows = new ArrayList<>();
        int rowLength = -1;
        for(int i = 0; i < list.size(); i++) {
            List<?> row = nonEmptyScalarSequence(list.get(i), desc + " element");
            if(i == 0) {
                rowLength = row.size();
            } else {
                assertEquals(
                    desc + " row " + (i + 1) + " has a different" + " length than previous rows:", rowLength, row.size()
                );
            }
            rows.add(row);
        }
        return rows;
    }

    /** Support comparing an expected value to an actual value. */
    interface CompareExpected
    {
        /**
         * Compares an actual value with the expected value represented by this object.
         *
         * @param actual the actual value
         * @return whether the actual value matches the expected value
         */
        boolean compareExpected(Object actual);
    }

    /** An object that represents a don't care value specified as an expected value. */
    static class DontCare implements CompareExpected
    {
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

    /** A class that represents a regular expression specified as an expected value. */
    static class Regexp implements CompareExpected
    {
        private final Pattern pattern;

        Regexp(String pattern) {
            this.pattern = Pattern.compile(convertPattern(pattern));
            LOG.debug("Regexp: '{}' => '{}'", pattern, this.pattern);
        }

        @Override
        public boolean compareExpected(Object actual) {
            boolean result = pattern.matcher(String.valueOf(actual)).matches();
            LOG.debug("Regexp.compareExpected pattern='{}', actual='{}' => '{}'", pattern, actual, result);
            return result;
        }

        @Override
        public String toString() {
            return "!re '" + pattern + "'";
        }
    }

    /** Convert a pattern from the input format, with {N} for captured groups, to the \N format used by Java regexps. */
    static String convertPattern(String pattern) {
        if(!pattern.contains("{")) {
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
            return pattern.replaceAll("(\\A|\\G|[^\\\\]|\\\\\\\\)[{]([0-9]+)[}]", "$1\\\\$2");
        }
    }

    /** A SnakeYAML constructor that converts dc tags to DontCare.INSTANCE and re tags to Regexp instances. */
    private static class RegisterTags extends SafeConstructor
    {
        private boolean recursing;

        RegisterTags() {
            yamlConstructors.put(new Tag("!dc"), new ConstructDontCare());
            yamlConstructors.put(new Tag("!re"), new ConstructRegexp());
            yamlConstructors.put(new Tag("!select-engine"), new ConstructSelectEngine());
            yamlConstructors.put(new Tag("!date"), new ConstructSystemDate());
            yamlConstructors.put(new Tag("!time"), new ConstructSystemTime());
            yamlConstructors.put(new Tag("!datetime"), new ConstructSystemDateTime());
            yamlConstructors.put(new Tag("!unicode"), new ConstructUnicode());
            yamlConstructors.put(new Tag("!utf8-bytes"), new ConstructUTF8Bytes());
        }

        @Override
        protected Object constructObject(Node node) {
            if (recursing)
                return super.constructObject(node);
            else {
                recursing = true;
                Object o = super.constructObject(node);
                recursing = false;
                return new LinedObject(o, node.getStartMark());
            }
        }

        private class LinedObject {
            private Object o;
            private Mark startMark;

            private LinedObject(Object o, Mark startMark) {
                this.o = o;
                this.startMark = startMark;
            }

            public Object getObject() {
                return o;
            }

            public Mark getStartMark() {
                return startMark;
            }
        }

        private static class ConstructDontCare extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                return DontCare.INSTANCE;
            }
        }

        private static class ConstructRegexp extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                if(!(node instanceof ScalarNode)) {
                    fail("The value of the regular expression (!re) tag must" + " be a scalar");
                }
                return new Regexp(((ScalarNode)node).getValue());
            }
        }

        private class ConstructSelectEngine extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                if(!(node instanceof MappingNode)) {
                    fail("The value of the !select-engine tag must be a map" + "\nGot: " + node);
                }
                String matchingKey = null;
                Object result = null;
                for(NodeTuple tuple : ((MappingNode)node).getValue()) {
                    Node keyNode = tuple.getKeyNode();
                    if(!(keyNode instanceof ScalarNode)) {
                        fail("The key in a !select-engine map must be a scalar" + "\nGot: " + constructObject(keyNode));
                    }
                    String key = ((ScalarNode)keyNode).getValue();
                    if(IT_ENGINE.equals(key) || (matchingKey == null && ALL_ENGINE.equals(key))) {
                        matchingKey = key;
                        result = constructObject(tuple.getValueNode());
                    }
                }
                if(matchingKey != null) {
                    LOG.debug("Select engine: '{}' => '{}'", matchingKey, result);
                    return result;
                } else {
                    LOG.debug("Select engine: no match");
                    return null;
                }
            }
        }

        private static class ConstructSystemDate extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                Date today = new Date();
                String str = DEFAULT_DATE_FORMAT.format(today);
                System.out.println("Constructed: " + today + " = " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z").format(today) +
                                   "; formatted as: " + str);
                return str;
            }

        }

        private static class ConstructSystemTime extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                return new TimeChecker();
            }
        }

        private static class ConstructSystemDateTime extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                return new DateTimeChecker();
            }
        }

        private static class ConstructUnicode extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                if(!(node instanceof ScalarNode)) {
                    fail("The value of the Unicode tag must be a scalar");
                }
                return ((ScalarNode)node).getValue();
            }
        }

        private static class ConstructUTF8Bytes extends AbstractConstruct
        {
            @Override
            public Object construct(Node node) {
                if(!(node instanceof ScalarNode)) {
                    fail("The value of the UTF8Bytes tag must be a scalar");
                }
                try {
                    return ((ScalarNode)node).getValue().getBytes("UTF-8");
                }
                catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

    }

    /** A class that compares a time value allowing a 1 minute window */
    static class TimeChecker implements CompareExpected
    {

        private static final int MINUTES_IN_SECONDS = 60;
        private static final int HOURS_IN_MINUTES = 60;

        @Override
        public boolean compareExpected(Object actual) {
            String[] timeAsString = String.valueOf(actual).split(":");
            Calendar localCalendar = Calendar.getInstance();
            localCalendar.setTimeInMillis(System.currentTimeMillis());

            long localTimeInSeconds = localCalendar.get(Calendar.HOUR_OF_DAY) * MINUTES_IN_SECONDS * HOURS_IN_MINUTES;
            localTimeInSeconds += localCalendar.get(Calendar.MINUTE) * MINUTES_IN_SECONDS;
            localTimeInSeconds += localCalendar.get(Calendar.SECOND);
            long resultTime = Integer.parseInt(timeAsString[0]) * MINUTES_IN_SECONDS * HOURS_IN_MINUTES;
            resultTime += Integer.parseInt(timeAsString[1]) * MINUTES_IN_SECONDS;
            resultTime += Integer.parseInt(timeAsString[2]);
            return Math.abs(resultTime - localTimeInSeconds) < MINUTES_IN_SECONDS;
        }

    }

    /** A class that compares a datetime value allowing a 1 minute window */
    static class DateTimeChecker implements CompareExpected
    {

        private static final int MINUTES_IN_SECONDS = 60;
        private static final int SECONDS_IN_MILLISECONDS = 1000;

        @Override
        public boolean compareExpected(Object actual) {
            Date now = new Date();
            Date date;
            try {
                date = DEFAULT_DATETIME_FORMAT.parse(String.valueOf(actual) + " UTC");
            } catch(ParseException e) {
                fail(e.getMessage());
                throw new AssertionError();
            }
            long testResult = date.getTime() - now.getTime();
            return Math.abs(testResult) < (1 * MINUTES_IN_SECONDS * SECONDS_IN_MILLISECONDS);
        }
    }
  
    private Integer getTypeNumber(String typeName) {
        try {
            Field f = java.sql.Types.class.getDeclaredField(typeName.toUpperCase());
            return f.getInt(null);
        } catch(NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError("Unable to get java.sql.Types named: " + typeName, e);
        }
    }

    /** An assertion error that includes context information. */
    private class ContextAssertionError extends AssertionError
    {
        ContextAssertionError(String failedStatement, String message) {
            super(context(failedStatement) + message);
        }

        ContextAssertionError(String failedStatement, String message, Throwable cause) {
            super(context(failedStatement) + message);
            initCause(cause);
        }

        public ContextAssertionError(String statement, AssertionError e) {
            super(context(statement) + e.getMessage());
            initCause(e);
        }
    }

    private class FullOutputAssertionError extends ComparisonFailure {
        FullOutputAssertionError(List<List<?>> expected, List<List<?>> actual, AssertionError e) {
            super(e.getMessage(), Strings.join(expected,"\n"), Strings.join(actual,"\n"));
        }

    }

    private String context(String failedStatement) {
        StringBuilder context = new StringBuilder();
        if(sourceURL != null) {
            context.append(sourceFilename());
        }
        if(!includeStack.isEmpty()) {
            for(URL include : includeStack) {
                if(context.length() != 0) {
                    context.append(", ");
                }
                context.append("Include ").append(include);
            }
        }
        if(commandNumber != 0) {
            if(context.length() != 0) {
                context.append(", ");
            }
            context.append("Command ").append(commandNumber);
            context.append(" at line ").append(lineNumber);
            if(commandName != null) {
                context.append(" (").append(commandName);
                if(failedStatement != null) {
                    context.append(": <").append(failedStatement).append('>');
                }
                context.append(')');
            }
        }
        if(context.length() != 0) {
            context.append(": ");
        }
        return context.toString();
    }

    private String sourceFilename() {
        String filename = sourceURL.toString();
        if (filename.startsWith("jar:file:")) {
            int idx = filename.indexOf("!/");
            if (idx > 0) {
                return filename.substring(idx+2);
            }
            return filename.substring(9);
        }
        if (filename.startsWith("file:")) {
            return filename.substring(5);
        }
        return filename;
    }

    private void jmxCommand(Object value, List<?> sequence) throws SQLException {
        if(value != null) {
            new JMXCommand(value, sequence).execute();
        }
    }

    private class JMXCommand extends AbstractStatementCommand
    {
        ArrayList<Object> output = null;
        ArrayList<Object> split_output = null;
        String objectName = null;
        String[] params = null;
        String method = null;
        String set = null;
        String get = null;

        public JMXCommand(Object value, List<?> sequence) {
            super(string(value, "JMX value"));
            if(value != null & String.valueOf(value).trim().length() > 1) {
                objectName = String.valueOf(value).trim();
            } else {
                fail("Must provide an Object name");
            }

            for(int i = 1; i < sequence.size(); i++) {
                Entry<?,?> map = onlyEntry(
                    sequence.get(i), "JMX attribute"
                );
                String attribute = string(map.getKey(), "JMX attribute name");
                Object attributeValue = map.getValue();
                if("params".equals(attribute)) {
                    parseParams(attributeValue);
                } else if("method".equals(attribute)) {
                    method = String.valueOf(attributeValue).trim();
                } else if("set".equals(attribute)) {
                    set = String.valueOf(attributeValue).trim();
                } else if("get".equals(attribute)) {
                    get = String.valueOf(attributeValue).trim();
                } else if("output".equals(attribute)) {
                    parseOutput(attributeValue);
                } else if("split_result".equals(attribute)) {
                    parseSplit(attributeValue);
                } else {
                    fail("The '" + attribute + "' attribute name was not" + " recognized");
                }
            }

        }

        private void parseParams(Object value) {
            if(value == null) {
                return;
            }
            assertNull("The params attribute must not appear more than once", params);
            ArrayList<String> list = new ArrayList<>(stringSequence(value, "params value"));
            params = list.toArray(new String[list.size()]);
        }

        private void parseSplit(Object value) {
            if(value == null) {
                split_output = null;
                return;
            }
            assertNull("The split_result attribute must not appear more than once", split_output);
            assertNull("The output and split_result attributes can not appear together", output);
            List<List<?>> rows = rows(value, "output split value");
            split_output = new ArrayList<>(rows.size());
            for(List<?> row : rows) {
                assertEquals("number of entries in row " + row, 1, row.size());
                split_output.add(row.get(0));
            }
        }

        private void parseOutput(Object value) {
            if(value == null) {
                output = null;
                return;
            }
            assertNull("The output attribute must not appear more than once", output);
            assertNull("The split_result and output attributes can not appear together", split_output);

            List<List<?>> rows = rows(value, "output value");
            output = new ArrayList<>(rows.size());
            for(List<?> row : rows) {
                assertEquals("number of entries in row " + row, 1, row.size());
                output.add(row.get(0));
            }
        }

        public void execute() {
            Object result = null;
            try(JMXInterpreter conn = new JMXInterpreter()) {
                if(method != null) {
                    result = conn.makeBeanCall("localhost", 8082, objectName, method, params, "method");
                }
                if(set != null) {
                    conn.makeBeanCall("localhost", 8082, objectName, set, params, "set");
                }
                if(get != null) {
                    result = conn.makeBeanCall("localhost", 8082, objectName, get, params, "get");
                }
            } catch(Exception e) {
                LOG.debug("Caught making JMX call", e);
                fail("Error: " + e.getMessage());
            }

            if(split_output != null) {
                if(result == null) {
                    fail("found null; expected: " + split_output);
                }
                List<Object> actual = new ArrayList<Object>(Arrays.asList(result.toString().split("\\n")));
                int highestCommon = Math.min(actual.size(), split_output.size());
                for(int i = 0; i < highestCommon; ++i) {
                    if(split_output.get(i) == DontCare.INSTANCE) {
                        actual.set(i, DontCare.INSTANCE);
                    }
                }
                assertCollectionEquals(split_output, actual);
            } else if(output != null) {
                if(result == null) {
                    fail("found null; expected: " + output);
                }
                List<Object> actual = new ArrayList<Object>(Arrays.asList(result.toString()));
                int highestCommon = Math.min(actual.size(), output.size());
                for(int i = 0; i < highestCommon; i++) {
                    if(output.get(i) == DontCare.INSTANCE) {
                        actual.set(i, DontCare.INSTANCE);
                    }
                }
                assertCollectionEquals(output, actual);
            }
        }
    }
}
