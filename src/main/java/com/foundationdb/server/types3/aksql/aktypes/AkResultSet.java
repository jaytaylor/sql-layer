/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.types3.aksql.aktypes;

import com.foundationdb.server.types3.Attribute;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TClassBase;
import com.foundationdb.server.types3.TClassFormatter;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TParser;
import com.foundationdb.server.types3.aksql.AkBundle;
import com.foundationdb.server.types3.aksql.AkCategory;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;

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
        TInstance instance = createInstanceNoArgs(false);
        instance.setMetaData(columns);
        return instance;
    }

    @Override
    public boolean attributeIsPhysical(int attributeIndex) {
        throw new UnsupportedOperationException();
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
    public TInstance instance(boolean nullable) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void validate(TInstance instance) {
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        throw new UnsupportedOperationException();
    }

    public TClass widestComparable()
    {
        return this;
    }
}
