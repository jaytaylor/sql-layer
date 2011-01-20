package com.akiban.cserver.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.util.ArgumentValidation;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class HapiGetRequest {

    public interface ParseErrorReporter {
        public void reportError(String error);
    }

    public static class Predicate {
        public enum Operator {
            EQ("=="),
            NE("!="),
            GT(">"),
            GTE(">="),
            LT("<"),
            LTE("<=")
            ;

            final private String toString;
            Operator(String toString) {
                this.toString = toString;
            }

            @Override
            public String toString() {
                return toString;
            }
        }
        private final TableName tableName;
        private final String columnName;
        private final Operator op;
        private final String value;

        Predicate(TableName tableName, String columnName, Operator op, String value) {
            ArgumentValidation.notNull("table name", tableName);
            ArgumentValidation.notNull("column name", columnName);
            ArgumentValidation.notNull("operator", op);
            this.tableName = tableName;
            this.columnName = columnName;
            this.op = op;
            this.value = value;
        }

        public TableName getTableName() {
            return tableName;
        }

        public String getColumnName() {
            return columnName;
        }

        public Operator getOp() {
            return op;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return appendToSB(new StringBuilder("Predicate["), null).append(']').toString();
        }

        public StringBuilder appendToSB(StringBuilder builder, TableName usingTable) {
            if (usingTable == null || !usingTable.equals(tableName)) {
                tableName.escape(builder, usingTable == null ? null : usingTable.getSchemaName());
                builder.append('.');
            }
            builder.append(columnName).append(op).append(value);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Predicate predicate = (Predicate) o;

            if (!columnName.equals(predicate.columnName)) return false;
            if (op != predicate.op) return false;
            if (!tableName.equals(predicate.tableName)) return false;
            if (value != null ? !value.equals(predicate.value) : predicate.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tableName.hashCode();
            result = 31 * result + columnName.hashCode();
            result = 31 * result + op.hashCode();
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

    HapiGetRequest()
    {}

    public static class HapiParseException extends RuntimeException {
        HapiParseException(Throwable cause) {
            super(cause);
        }
    }

    public static HapiGetRequest parse(String query) {
        return parse(query, null);
    }
    
    public static HapiGetRequest parse(String query, ParseErrorReporter errorReporter) {
        hapiLexer lexer = new hapiLexer( new ANTLRStringStream(query) );
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        hapiParser parser = new hapiParser(tokens);
        parser.setErrorReporter(errorReporter);
        try {
            return parser.get_request();
        } catch (hapiLexer.HapiLexerException e) {
            throw new HapiParseException(e);
        } catch (RecognitionException e) {
            throw new HapiParseException(e);
        }
    }

    private String schema;
    private String table;
    private String usingTable;
    private TableName usingTableName;
    private final List<Predicate> predicates = new ArrayList<Predicate>();

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public TableName getUsingTable() {
        if (usingTableName == null) {
            usingTableName = new TableName(getSchema(), usingTable);
        }
        return usingTableName;
    }

    public List<Predicate> getPredicates() {
        return Collections.unmodifiableList(predicates);
    }

    void setTable(String table) {
        this.table = table;
        this.usingTable = table;
    }

    void setUsingTable(String usingTable) {
        this.usingTable = usingTable;
    }

    void setSchema(String schema) {
        this.schema = schema;
    }

    void addPredicate(String columnName, Predicate.Operator operator, String value) {
        predicates.add( new Predicate(getUsingTable(), columnName, operator, value) );
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("HapiGetRequest[ ");
        builder.append(schema).append(':').append(table).append(':');
        final TableName using = getUsingTable();
        if (!using.getTableName().equals(getTable())) {
            builder.append('(').append(using.getTableName()).append(')');
        }
        for (Predicate predicate : predicates) {
            predicate.appendToSB(builder, using).append(',');
        }
        builder.setLength(builder.length()-1);
        builder.append(" ]");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HapiGetRequest that = (HapiGetRequest) o;

        if (!predicates.equals(that.predicates)) return false;
        if (!schema.equals(that.schema)) return false;
        if (!table.equals(that.table)) return false;
        if (!usingTable.equals(that.usingTable)) return false;
        if (!usingTableName.equals(that.usingTableName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + usingTable.hashCode();
        result = 31 * result + usingTableName.hashCode();
        result = 31 * result + predicates.hashCode();
        return result;
    }

    void validate() {
        List<String> errors = null;
        if (schema == null) {
            errors = error("schema is null", errors);
        }
        if (table == null) {
            errors = error("table is null", errors);
        }
        if (usingTable == null) {
            errors = error("usingTable is null", errors);
        }
        if(usingTableName == null) {
            errors = error("usingTableName is null", errors);
        }
        else if( usingTable != null && schema != null ) {
            if (!(usingTableName.getSchemaName().equals(schema) && usingTableName.getTableName().equals(usingTable))) {
                errors = error(usingTableName + "!=" + new TableName(schema, usingTable), errors);
            }
        }
        if(predicates.isEmpty()) {
            errors = error("predicates are empty", errors);
        }
        if (errors != null) {
            StringBuilder err = new StringBuilder("internal error");
            if (errors.size() != 1) {
                err.append('s');
            }
            err.append(": ").append(errors);
            throw new IllegalStateException(err.toString());
        }
    }

    private static List<String> error(String error, List<String> errors) {
        if (errors == null) {
            errors = new ArrayList<String>();
        }
        errors.add(error);
        return errors;
    }
}
