/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.types.aksql.aktypes;

import com.foundationdb.server.error.UnsupportedSpatialCast;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TClassBase;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;

import java.sql.Types;

/** An internal, runtime only wrapper around JTS Geometry objects. */
public class AkGeometry extends TClassBase
{
    public static final TClass INSTANCE = new AkGeometry();

    private static final String NAME = "GEOMETRY";
    private static final TypeId TYPE_ID;
    private static final DataTypeDescriptor DATA_TYPE_DESCRIPTOR;

    static {
        try {
            TYPE_ID =  TypeId.getUserDefinedTypeId(NAME, false);
            DATA_TYPE_DESCRIPTOR = new DataTypeDescriptor(TYPE_ID, true);
        } catch(StandardException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class GeometryParser implements TParser
    {
        @Override
        public void parse(TExecutionContext context, ValueSource in, ValueTarget out) {
            throw new UnsupportedSpatialCast();
        }
    }

    private static class GeometryFormatter implements TClassFormatter
    {
        @Override
        public void format(TInstance type, ValueSource source, AkibanAppender out) {
            throw new UnsupportedSpatialCast();
        }

        @Override
        public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
            throw new UnsupportedSpatialCast();
        }

        @Override
        public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
            throw new UnsupportedSpatialCast();
        }
    }

    private AkGeometry() {
        super(AkBundle.INSTANCE.id(),
              NAME,
              null /*category*/,
              Attribute.NONE.class,
              new GeometryFormatter(),
              1 /*internal version*/,
              1 /*version*/,
              0 /*size*/,
              null /*underlying type*/,
              new GeometryParser(),
              -1 /*default varchar len*/);
    }

    @Override
    public int jdbcType() {
        return Types.OTHER;
    }

    @Override
    protected DataTypeDescriptor dataTypeDescriptor(TInstance type) {
        return DATA_TYPE_DESCRIPTOR;
    }

    @Override
    public TClass widestComparable() {
        return this;
    }

    @Override
    protected boolean attributeIsPhysical(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean attributeAlwaysDisplayed(int attributeIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TInstance instance(boolean nullable) {
        return createInstanceNoArgs(nullable);
    }

    @Override
    protected TInstance doPickInstance(TInstance left, TInstance right, boolean suggestedNullability) {
        return createInstanceNoArgs(suggestedNullability);
    }

    @Override
    protected void validate(TInstance type) {
    }
}
