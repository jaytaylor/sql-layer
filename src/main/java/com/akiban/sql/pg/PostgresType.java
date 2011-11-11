/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.server.error.UnknownDataTypeException;
import com.akiban.server.error.UnknownTypeSizeException;
import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.types.AkType;

import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;

import java.io.*;
import java.text.*;
import java.util.*;

/** A type according to the PostgreSQL regime.
 * Information corresponds more-or-less directly to what's in the 
 * <code>pg_attribute</code> table.
 */
public class PostgresType
{
    /*** Type OIDs ***/

    public static final int BOOL_TYPE_OID = 16;
    public static final int BYTEA_TYPE_OID = 17;
    public static final int CHAR_TYPE_OID = 18;
    public static final int NAME_TYPE_OID = 19;
    public static final int INT8_TYPE_OID = 20;
    public static final int INT2_TYPE_OID = 21;
    public static final int INT2VECTOR_TYPE_OID = 22;
    public static final int INT4_TYPE_OID = 23;
    public static final int REGPROC_TYPE_OID = 24;
    public static final int TEXT_TYPE_OID = 25;
    public static final int OID_TYPE_OID = 26;
    public static final int TID_TYPE_OID = 27;
    public static final int XID_TYPE_OID = 28;
    public static final int CID_TYPE_OID = 29;
    public static final int OIDVECTOR_TYPE_OID = 30;
    public static final int PG_TYPE_TYPE_OID = 71;
    public static final int PG_ATTRIBUTE_TYPE_OID = 75;
    public static final int PG_PROC_TYPE_OID = 81;
    public static final int PG_CLASS_TYPE_OID = 83;
    public static final int XML_TYPE_OID = 142;
    public static final int _XML_TYPE_OID = 143;
    public static final int SMGR_TYPE_OID = 210;
    public static final int POINT_TYPE_OID = 600;
    public static final int LSEG_TYPE_OID = 601;
    public static final int PATH_TYPE_OID = 602;
    public static final int BOX_TYPE_OID = 603;
    public static final int POLYGON_TYPE_OID = 604;
    public static final int LINE_TYPE_OID = 628;
    public static final int _LINE_TYPE_OID = 629;
    public static final int FLOAT4_TYPE_OID = 700;
    public static final int FLOAT8_TYPE_OID = 701;
    public static final int ABSTIME_TYPE_OID = 702;
    public static final int RELTIME_TYPE_OID = 703;
    public static final int TINTERVAL_TYPE_OID = 704;
    public static final int UNKNOWN_TYPE_OID = 705;
    public static final int CIRCLE_TYPE_OID = 718;
    public static final int _CIRCLE_TYPE_OID = 719;
    public static final int MONEY_TYPE_OID = 790;
    public static final int _MONEY_TYPE_OID = 791;
    public static final int MACADDR_TYPE_OID = 829;
    public static final int INET_TYPE_OID = 869;
    public static final int CIDR_TYPE_OID = 650;
    public static final int _BOOL_TYPE_OID = 1000;
    public static final int _BYTEA_TYPE_OID = 1001;
    public static final int _CHAR_TYPE_OID = 1002;
    public static final int _NAME_TYPE_OID = 1003;
    public static final int _INT2_TYPE_OID = 1005;
    public static final int _INT2VECTOR_TYPE_OID = 1006;
    public static final int _INT4_TYPE_OID = 1007;
    public static final int _REGPROC_TYPE_OID = 1008;
    public static final int _TEXT_TYPE_OID = 1009;
    public static final int _OID_TYPE_OID = 1028;
    public static final int _TID_TYPE_OID = 1010;
    public static final int _XID_TYPE_OID = 1011;
    public static final int _CID_TYPE_OID = 1012;
    public static final int _OIDVECTOR_TYPE_OID = 1013;
    public static final int _BPCHAR_TYPE_OID = 1014;
    public static final int _VARCHAR_TYPE_OID = 1015;
    public static final int _INT8_TYPE_OID = 1016;
    public static final int _POINT_TYPE_OID = 1017;
    public static final int _LSEG_TYPE_OID = 1018;
    public static final int _PATH_TYPE_OID = 1019;
    public static final int _BOX_TYPE_OID = 1020;
    public static final int _FLOAT4_TYPE_OID = 1021;
    public static final int _FLOAT8_TYPE_OID = 1022;
    public static final int _ABSTIME_TYPE_OID = 1023;
    public static final int _RELTIME_TYPE_OID = 1024;
    public static final int _TINTERVAL_TYPE_OID = 1025;
    public static final int _POLYGON_TYPE_OID = 1027;
    public static final int ACLITEM_TYPE_OID = 1033;
    public static final int _ACLITEM_TYPE_OID = 1034;
    public static final int _MACADDR_TYPE_OID = 1040;
    public static final int _INET_TYPE_OID = 1041;
    public static final int _CIDR_TYPE_OID = 651;
    public static final int _CSTRING_TYPE_OID = 1263;
    public static final int BPCHAR_TYPE_OID = 1042;
    public static final int VARCHAR_TYPE_OID = 1043;
    public static final int DATE_TYPE_OID = 1082;
    public static final int TIME_TYPE_OID = 1083;
    public static final int TIMESTAMP_TYPE_OID = 1114;
    public static final int _TIMESTAMP_TYPE_OID = 1115;
    public static final int _DATE_TYPE_OID = 1182;
    public static final int _TIME_TYPE_OID = 1183;
    public static final int TIMESTAMPTZ_TYPE_OID = 1184;
    public static final int _TIMESTAMPTZ_TYPE_OID = 1185;
    public static final int INTERVAL_TYPE_OID = 1186;
    public static final int _INTERVAL_TYPE_OID = 1187;
    public static final int _NUMERIC_TYPE_OID = 1231;
    public static final int TIMETZ_TYPE_OID = 1266;
    public static final int _TIMETZ_TYPE_OID = 1270;
    public static final int BIT_TYPE_OID = 1560;
    public static final int _BIT_TYPE_OID = 1561;
    public static final int VARBIT_TYPE_OID = 1562;
    public static final int _VARBIT_TYPE_OID = 1563;
    public static final int NUMERIC_TYPE_OID = 1700;
    public static final int REFCURSOR_TYPE_OID = 1790;
    public static final int _REFCURSOR_TYPE_OID = 2201;
    public static final int REGPROCEDURE_TYPE_OID = 2202;
    public static final int REGOPER_TYPE_OID = 2203;
    public static final int REGOPERATOR_TYPE_OID = 2204;
    
    /*** Representation. ***/
    private int oid;
    private short length;
    private int modifier;
    private LongExtractor converter;

    public PostgresType(int oid, short length, int modifier) {
        this.oid = oid;
        this.length = length;
        this.modifier = modifier;
    }

    public int getOid() {
        return oid;
    }
    public short getLength() {
        return length;
    }
    public int getModifier() {
        return modifier;
    }

    public static PostgresType fromAIS(Column aisColumn) {
        return fromAIS(aisColumn.getType(), aisColumn);
    }
        
    public static PostgresType fromAIS(Type aisType, Column aisColumn)  {
        int oid;
        short length = -1;
        int modifier = -1;

        String encoding = aisType.encoding();

        if ("VARCHAR".equals(encoding))
            oid = VARCHAR_TYPE_OID;
        else if ("INT".equals(encoding) ||
                 "U_INT".equals(encoding) ||
                 "U_BIGINT".equals(encoding)) {
            switch (aisType.maxSizeBytes().intValue()) {
            case 1:
                oid = INT2_TYPE_OID; // No INT1; this also could be BOOLEAN (TINYINT(1)).
                break;
            case 2:
            case 3:
                oid = INT2_TYPE_OID;
                break;
            case 4:
                oid = INT4_TYPE_OID;
                break;
            case 8:
                oid = INT8_TYPE_OID;
                break;
            default:
                throw new UnknownTypeSizeException (aisType);
            }
        }
        else if ("DATE".equals(encoding))
            oid = DATE_TYPE_OID;
        else if ("TIME".equals(encoding))
            oid = TIME_TYPE_OID;
        else if ("DATETIME".equals(encoding) ||
                 "TIMESTAMP".equals(encoding))
            oid = TIMESTAMP_TYPE_OID;
        else if ("BLOB".equals(encoding) ||
                 "TEXT".equals(encoding))
            oid = TEXT_TYPE_OID;
        else if ("YEAR".equals(encoding))
            oid = INT2_TYPE_OID; // No INT1
        else if ("DECIMAL".equals(encoding) ||
                 "U_DECIMAL".equals(encoding))
            oid = NUMERIC_TYPE_OID;
        else if ("FLOAT".equals(encoding) ||
                 "U_FLOAT".equals(encoding))
            oid = FLOAT4_TYPE_OID;
        else if ("DOUBLE".equals(encoding) ||
                 "U_DOUBLE".equals(encoding))
            oid = FLOAT8_TYPE_OID;
        else
            throw new UnknownDataTypeException (encoding);

        if (aisType.fixedSize())
            length = aisType.maxSizeBytes().shortValue();

        if (aisColumn != null) {
            switch (aisType.nTypeParameters()) {
            case 1:
                modifier = aisColumn.getTypeParameter1().intValue();
                break;
            }
        }

        PostgresType result = new PostgresType(oid, length, modifier);

        // TODO: For now, these are the only ones needing special treatment.
        // When we are better able to work with the encoder to get the
        // raw bytes, can use this for all.
        result.converter = Extractors.getLongExtractor(aisType.akType());

        return result;
    }

    public static PostgresType fromDerby(DataTypeDescriptor type)  {
        int oid;
        short length = -1;
        int modifier = -1;

        TypeId typeId = type.getTypeId();

        LongExtractor converter = null;

        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.BIT_TYPE_ID:
            oid = BIT_TYPE_OID;
            break;
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            oid = BOOL_TYPE_OID;
            break;
        case TypeId.FormatIds.CHAR_TYPE_ID:
            oid = CHAR_TYPE_OID;
            break;
        case TypeId.FormatIds.DATE_TYPE_ID:
            oid = DATE_TYPE_OID;
            converter = Extractors.getLongExtractor(AkType.DATE);
            break;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            oid = NUMERIC_TYPE_OID;
            break;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            oid = FLOAT8_TYPE_OID;
            break;
        case TypeId.FormatIds.INT_TYPE_ID:
            oid = INT4_TYPE_OID;
            converter = Extractors.getLongExtractor(AkType.INT);
            break;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            oid = INT8_TYPE_OID;
            converter = Extractors.getLongExtractor(AkType.INT);
            break;
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
            oid = TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
            oid = TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.REAL_TYPE_ID:
            oid = FLOAT4_TYPE_OID;
            break;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            oid = INT2_TYPE_OID;
            converter = Extractors.getLongExtractor(AkType.INT);
            break;
        case TypeId.FormatIds.TIME_TYPE_ID:
            oid = TIME_TYPE_OID;
            converter = Extractors.getLongExtractor(AkType.TIME);
            break;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            oid = TIMESTAMP_TYPE_OID;
            converter = Extractors.getLongExtractor(AkType.TIMESTAMP);
            break;
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            oid = INT2_TYPE_OID; // No INT1
            break;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            oid = VARBIT_TYPE_OID;
            break;
        case TypeId.FormatIds.BLOB_TYPE_ID:
            oid = TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            oid = VARCHAR_TYPE_OID;
            break;
        case TypeId.FormatIds.CLOB_TYPE_ID:
            oid = TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.XML_TYPE_ID:
            oid = XML_TYPE_OID;
            break;
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
            {
                // Might be a type known to AIS but not to Derby.
                // TODO: Need to reconcile.
                String name = typeId.getSQLTypeName();
                for (Type aisType : Types.types()) {
                    if (aisType.name().equals(name)) {
                        return fromAIS(aisType, null);
                    }
                }
            }
            /* falls through */
        default:
            throw new UnknownDataTypeException(type.toString());
        }

        if (typeId.isDecimalTypeId() || typeId.isNumericTypeId()) {
            length = (short)type.getPrecision();
            modifier = type.getScale();
        }
        else if (typeId.variableLength()) {
            modifier = type.getMaximumWidth();
        }
        else {
            length = (short)typeId.getMaximumMaximumWidth();
        }
        
        PostgresType result = new PostgresType(oid, length, modifier);
        
        if (converter != null)
            result.converter = converter;

        return result;
    }

    public static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    public static final DateFormat timeFormatter = new SimpleDateFormat("hh:mm:ss");
    public static final DateFormat datetimeFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    public byte[] encodeValue(Object value, String encoding, boolean binary) 
            throws IOException {
        if (value == null)
            return null;
        try {
            if (binary) {
                throw new UnsupportedCharsetException ("", "", "BINARY");
            }
            else if (converter != null) {
                value = converter.asString((Long)value);
            }
            else if (value instanceof Date) {
                DateFormat format = null;
                switch (oid) {
                case DATE_TYPE_OID:
                    format = dateFormatter;
                    break;
                case TIME_TYPE_OID:
                    format = timeFormatter;
                    break;
                case TIMESTAMP_TYPE_OID:
                    format = datetimeFormatter;
                    break;
                }
                if (format != null)
                    value = format.format((Date)value);
            }
            return value.toString().getBytes(encoding);
        }
        catch (UnsupportedEncodingException ex) {
            throw new UnsupportedCharsetException ("", "", encoding);
        }
    }

    public Object decodeParameter(String value) {
        if (converter != null)
            return converter.getLong(value);
        else
            return value;
    }

}
