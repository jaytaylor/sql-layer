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

package com.akiban.server.service.memcache;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.hapi.HapiUtils;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ParsedHapiGetRequest implements HapiGetRequest {

    public interface ParseErrorReporter {
        public void reportError(String error);
    }

    private final static ParseErrorReporter ERROR_REPORTER = new ParsedHapiGetRequest.ParseErrorReporter() {
        @Override
        public void reportError(String error) {
            throw new HapiParseException(error);
        }
    };

    ParsedHapiGetRequest()
    {}

    public static class HapiParseException extends RuntimeException {
        public HapiParseException(String message) {
            super(message);
        }

        HapiParseException(Throwable cause) {
            super(cause);
        }
    }

    public static HapiGetRequest parse(String query) throws HapiRequestException {
        return parse(query, ERROR_REPORTER);
    }
    
    public static ParsedHapiGetRequest parse(String query, ParseErrorReporter errorReporter)
            throws HapiRequestException
    {
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
        } catch (HapiParseException e) {
            throw new HapiRequestException("while parsing request", e, HapiRequestException.ReasonCode.UNPARSABLE);
        }
    }

    private String schema;
    private String table;
    private String usingTable;
    private TableName usingTableName;
    private int limit = -1;
    private final List<HapiPredicate> predicates = new ArrayList<HapiPredicate>();

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public TableName getUsingTable() {
        if (usingTableName == null) {
            usingTableName = new TableName(getSchema(), usingTable);
        }
        return usingTableName;
    }

    @Override
    public List<HapiPredicate> getPredicates() {
        return Collections.unmodifiableList(predicates);
    }

    @Override
    public int getLimit() {
        return limit;
    }

    void setLimit(String limit) {
        this.limit = Integer.parseInt(limit);
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

    void addPredicate(String columnName, SimpleHapiPredicate.Operator operator, String value) {
        predicates.add( new SimpleHapiPredicate(getUsingTable(), columnName, operator, value) );
    }

    @Override
    public String toString() {
        return HapiUtils.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof HapiGetRequest && HapiUtils.equals(this, (HapiGetRequest) o);
    }

    @Override
    public int hashCode() {
        return HapiUtils.hashCode(this);
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
