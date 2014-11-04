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

package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import java.sql.Types;

public class IndexedField
{
    public static final String KEY_FIELD = ".hkey";

    public static enum FieldType {
        INT, LONG, FLOAT, DOUBLE, STRING, TEXT
    }

    private final Column column;
    private final int position;
    private final String name;
    private final FieldType fieldType;
    
    public IndexedField(Column column) {
        this.column = column;

        position = column.getPosition();
        name = column.getName(); // TODO: Need to make unique among multiple tables.
        
        switch (column.getType().typeClass().jdbcType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            fieldType = FieldType.INT;
            break;
        case Types.BIGINT:
            fieldType = FieldType.LONG;
            break;
        case Types.FLOAT:
        case Types.REAL:
            fieldType = FieldType.FLOAT;
            break;
        case Types.DOUBLE:
            fieldType = FieldType.DOUBLE;
            break;
        case Types.CHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
            {
                AkCollator collator = column.getCollator();
                if ((collator == null) || collator.isCaseSensitive()) {
                    fieldType = FieldType.STRING;
                }
                else {
                    fieldType = FieldType.TEXT;
                }
            }
            break;
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.CLOB:
            fieldType = FieldType.TEXT;
            break;
        default:
            fieldType = FieldType.STRING;
            break;
        }
    }

    public Column getColumn() {
        return column;
    }

    public int getPosition() {
        return position;
    }

    public String getName() {
        return name;
    }

    public boolean isCasePreserving() {
        return (fieldType != FieldType.TEXT);
    }

    public Field getField(ValueSource value) {
        if (value.isNull()) 
            return null;
        Field.Store store = Field.Store.NO; // Only store hkey.
        switch (fieldType) {
        case INT:
            switch (TInstance.underlyingType(value.getType())) {
            case INT_8:
                return new IntField(name, value.getInt8(), store);
            case INT_16:
                return new IntField(name, value.getInt16(), store);
            case UINT_16:
                return new IntField(name, value.getUInt16(), store);
            case INT_32:
            default:
                return new IntField(name, value.getInt32(), store);
            }
        case LONG:
            return new LongField(name, value.getInt64(), store);
        case FLOAT:
            return new FloatField(name, value.getFloat(), store);
        case DOUBLE:
            return new DoubleField(name, value.getDouble(), store);
        case STRING:
            switch (TInstance.underlyingType(value.getType())) {
            case STRING:
                return new StringField(name, value.getString(), store);
            default:
                {
                    StringBuilder str = new StringBuilder();
                    value.getType().format(value, AkibanAppender.of(str));
                    return new StringField(name, str.toString(), store);
                }
            }
        case TEXT:
            return new TextField(name, value.getString(), store);
        default:
            return null;
        }
    }

    @Override
    public String toString() {
        return name;
    }

}
