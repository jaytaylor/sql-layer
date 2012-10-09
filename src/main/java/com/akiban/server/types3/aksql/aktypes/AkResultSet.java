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

package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TClassBase;
import com.akiban.server.types3.TClassFormatter;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TParser;
import com.akiban.server.types3.aksql.AkBundle;
import com.akiban.server.types3.aksql.AkCategory;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;

import java.util.List;

public class AkResultSet extends TClassBase {
    public static class Column {
        private final String name;
        private final TInstance type;
        
        public Column(String name, TInstance type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public TInstance getType() {
            return type;
        }
    }

    private static final TClassFormatter NO_FORMATTER = new TClassFormatter() {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
                throw new UnsupportedOperationException();
            }
        };

    private static final TParser NO_PARSER = new TParser() {
            @Override
            public void parse(TExecutionContext context, PValueSource in, PValueTarget out) {
                throw new UnsupportedOperationException();
            }
        };

    /**
     * A result set instance, which does not obey all of the scalar type protocol.
     */
    public static final AkResultSet INSTANCE = new AkResultSet();

    private AkResultSet() {
        super(AkBundle.INSTANCE.id(),
              "result set",
              AkCategory.RECORD,
              Attribute.NONE.class,
              NO_FORMATTER,
              1,
              1,
              0,
              null, // PUnderlying.XXX
              NO_PARSER,
              -1);
    }

    public TInstance instance(List<Column> columns) {
        TInstance instance = createInstanceNoArgs();
        instance.setMetaData(columns);
        return instance;
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance instance) {
        List<Column> columns = (List<Column>)instance.getMetaData();
        String[] columnNames = new String[columns.size()];
        DataTypeDescriptor[] columnTypes = new DataTypeDescriptor[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnNames[i] = columns.get(i).getName();
            columnTypes[i] = columns.get(i).getType().dataTypeDescriptor();
        }
        TypeId typeId = new TypeId.RowMultiSetTypeId(columnNames, columnTypes);
        Boolean isNullable = instance.nullability();
        return new DataTypeDescriptor(typeId, isNullable);
    }

    @Override
    public TInstance instance() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void validate(TInstance instance) {
    }


    @Override
    protected TInstancePicker defaultPicker() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TFactory factory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putSafety(TExecutionContext context, 
                          TInstance sourceInstance,
                          PValueSource sourceValue,
                          TInstance targetInstance,
                          PValueTarget targetValue) {
    }
}
