package com.akiban.ais.ddl;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.staticgrouping.Grouping;
import com.akiban.util.MySqlStatementSplitter;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DDLGroupingConverter {

    public static class FFResult {
        private final boolean sawCreate;
        private final String readString;

        private FFResult(boolean sawCreate, String readString) {
            this.sawCreate = sawCreate;
            this.readString = readString;
        }

        public static FFResult noCreate(String string) {
            return new FFResult(false, string);
        }

        public static FFResult withCreate(String string) {
            return new FFResult(true, string);
        }

        public boolean sawCreate() {
            return sawCreate;
        }

        public String getReadString() {
            return readString;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FFResult ffResult = (FFResult) o;
            return sawCreate == ffResult.sawCreate && readString.equals(ffResult.readString);
        }

        @Override
        public int hashCode() {
            int result = (sawCreate ? 1 : 0);
            result = 31 * result + readString.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "FFResult[" + sawCreate + ", " + readString + ']';
        }
    }

    private final Set<String> seenSchemas = new HashSet<String>();

    public static void convert(Reader in, Writer out) throws Exception {
        DDLGroupingConverter converter = new DDLGroupingConverter(in, out);
        converter.convert();
    }

    /**
     * Does a conversion. First arg is required, and is the input file name. Second arg is optional; if provided,
     * it's the output file name, and if not, it'll replace the input file. All other args are ignored.
     * @param args see above
     * @throws Exception if a file couldn't be read from or written to
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Require at least one arg.");
            System.exit(1);
        }
        String inFile = args[0];
        String outFile = args.length > 1 && args[1] != null && args[1].trim().length() > 0
                ? args[1]
                : inFile;

        final String converted;
        Reader inReader = new FileReader(inFile);
        try {
            StringWriter writer = new StringWriter();
            convert(inReader, writer);
            writer.flush();
            converted = writer.toString();
        } finally {
            inReader.close();
        }

        Writer fileWriter = new FileWriter(outFile, false);
        try {
            fileWriter.write(converted);
        } finally {
            fileWriter.close();
        }

        System.out.println("Done");
    }

    private final static Pattern REGEX = Pattern.compile("\\s*create\\s+table\\s+(?:(`?)([\\w]+)\\1\\s*\\.\\s*)?(`?)([\\w]+)\\3", Pattern.CASE_INSENSITIVE);
    private final static String CREATE_TABLE = "create table";
    final BufferedReader in;
    final PrintWriter out;

    public DDLGroupingConverter(Reader in, Writer out) {
        this.in = new BufferedReader(in);
        this.out = new PrintWriter(out);
    }

    public void convert() throws Exception {
        boolean firstIteration = true;
        FFResult result;
        String defaultSchema = null;
        Map<TableName,String> fkStrings = null;
        while (null != (result = fastForward(in))) {
            if (firstIteration) {
                firstIteration = false;
                Grouping grouping = OldGroupingReader.fromString(result.getReadString());
                fkStrings = grouping.traverse(new FKMaker());
                defaultSchema = grouping.getDefaultSchema();
            }
            out.print(result.getReadString());
            if (result.sawCreate()) {
                MySqlStatementSplitter splitter = new MySqlStatementSplitter(in, null, true, false, null);
                String spliced;
                while (null != (spliced = splice(fkStrings, splitter, defaultSchema))) {
                    out.print(spliced);
                }
            }
        }
    }

    // Yikes, this is ugly!
    private TableName getCreatedTable(String statement, String defaultSchema, StringBuilder outBuilder) {
        Matcher matcher = REGEX.matcher(statement);
        if (!matcher.find()) {
            throw new RuntimeException("couldn't find regex <" + REGEX + "> in string: " + statement);
        }
        String schema = matcher.group(2);
        if (schema == null) {
            schema = defaultSchema;
        }
        if (seenSchemas.add(schema)) {
            assert outBuilder.length() == 0 : outBuilder.length();
            outBuilder.append("use ");
            TableName.escape(schema, outBuilder);
            outBuilder.append(";\n\n").append(statement);
        }
        String table = matcher.group(4);
        return TableName.create(schema, table);
    }

    private String splice(Map<TableName,String> fkStrings, MySqlStatementSplitter splitter, String defaultSchema) {
        String statement = splitter.parse();
        if (statement == null) {
            return null;
        }

        StringBuilder statementEditor = new StringBuilder();
        TableName whichTable = getCreatedTable(statement, defaultSchema, statementEditor);
        if (statementEditor.length() > 0) {
            statement = statementEditor.toString();
        }

        String result = fkStrings.get(whichTable);
        if (result != null) {
            if (result != null) {
                StringBuilder builder = new StringBuilder(statement);
                builder.reverse();
                int insertAt = builder.indexOf(")");
                builder.reverse();
                insertAt = builder.length() - (insertAt + 1);
                if (builder.charAt(insertAt-1) == '\n') {
                    --insertAt;
                }
                builder.insert(insertAt, result);

                statement = builder.toString();
            }
        }
        return statement;
    }

    static FFResult fastForward(Reader in) throws IOException {
        StringBuilder read = new StringBuilder();
        int strIndex = 0;
        in.mark(CREATE_TABLE.length());
        while (in.ready()) {
            int charInt = in.read();
            if (charInt < 0) {
                return read.length() == 0 ? null : FFResult.noCreate(read.toString());
            }
            char c = (char) charInt;
            read.append(c);
            if (Character.toLowerCase(c) == CREATE_TABLE.charAt(strIndex)) {
                if (++strIndex == CREATE_TABLE.length()) {
                    in.reset();
                    read.setLength(read.length() - CREATE_TABLE.length());
                    return FFResult.withCreate(read.toString());
                }
                assert strIndex < CREATE_TABLE.length() : strIndex ;
            }
            else {
                in.mark(CREATE_TABLE.length());
                strIndex = 0;
            }
        }
        return read.length() == 0 ? null : FFResult.noCreate(read.toString());
    }
}
