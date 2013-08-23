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
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.Types;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.util.AkibanAppender;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

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
        
        Type columnType = column.getType();
        if (columnType.equals(Types.INT) ||
            columnType.equals(Types.MEDIUMINT) || columnType.equals(Types.U_MEDIUMINT) ||
            columnType.equals(Types.SMALLINT) || columnType.equals(Types.U_SMALLINT) ||
            columnType.equals(Types.TINYINT) || columnType.equals(Types.U_TINYINT)) {
            fieldType = FieldType.INT;
        }
        else if (columnType.equals(Types.BIGINT) || columnType.equals(Types.U_INT)) {
            fieldType = FieldType.LONG;
        }
        else if (columnType.equals(Types.DOUBLE) || columnType.equals(Types.U_DOUBLE)) {
            fieldType = FieldType.DOUBLE;
        }
        else if (columnType.equals(Types.VARCHAR) || columnType.equals(Types.CHAR)) {
            AkCollator collator = column.getCollator();
            if ((collator == null) || collator.isCaseSensitive()) {
                fieldType = FieldType.STRING;
            }
            else {
                fieldType = FieldType.TEXT;
            }
        }
        else if (columnType.equals(Types.TEXT) || columnType.equals(Types.TINYTEXT) ||
                 columnType.equals(Types.MEDIUMTEXT) || columnType.equals(Types.LONGTEXT)) {
            fieldType = FieldType.TEXT;
        }
        else {
            fieldType = FieldType.STRING;
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

    public Field getField(PValueSource value) {
        if (value.isNull()) 
            return null;
        Field.Store store = Field.Store.NO; // Only store hkey.
        switch (fieldType) {
        case INT:
            switch (TInstance.pUnderlying(value.tInstance())) {
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
            switch (TInstance.pUnderlying(value.tInstance())) {
            case STRING:
                return new StringField(name, value.getString(), store);
            default:
                {
                    StringBuilder str = new StringBuilder();
                    value.tInstance().format(value, AkibanAppender.of(str));
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
