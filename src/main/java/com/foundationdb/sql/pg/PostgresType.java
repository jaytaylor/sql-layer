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

package com.foundationdb.sql.pg;

import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.sql.server.ServerType;

import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.UnknownDataTypeException;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TBinary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

/** A type according to the PostgreSQL regime.
 * Information corresponds more-or-less directly to what's in the 
 * <code>pg_attribute</code> table.
 */
public class PostgresType extends ServerType
{
    private static Logger logger = LoggerFactory.getLogger(PostgresType.class);

    /*** Type OIDs ***/

    public enum TypeOid {

        BOOL_TYPE_OID(16, "bool", BinaryEncoding.BOOLEAN_C, Types.BOOLEAN),
        BYTEA_TYPE_OID(17, "bytea", BinaryEncoding.BINARY_OCTAL_TEXT, Types.VARBINARY),
        CHAR_TYPE_OID(18, "char", BinaryEncoding.STRING_BYTES, Types.CHAR),
        NAME_TYPE_OID(19, "name"),
        INT8_TYPE_OID(20, "int8", BinaryEncoding.INT_64, Types.BIGINT),
        INT2_TYPE_OID(21, "int2", BinaryEncoding.INT_16, Types.SMALLINT),
        INT2VECTOR_TYPE_OID(22, "int2vector"),
        INT4_TYPE_OID(23, "int4", BinaryEncoding.INT_32, Types.INTEGER),
        REGPROC_TYPE_OID(24, "regproc"),
        TEXT_TYPE_OID(25, "text"),
        OID_TYPE_OID(26, "oid"),
        TID_TYPE_OID(27, "tid"),
        XID_TYPE_OID(28, "xid"),
        CID_TYPE_OID(29, "cid"),
        OIDVECTOR_TYPE_OID(30, "oidvector"),
        PG_TYPE_TYPE_OID(71, "pg_type", TypType.COMPOSITE),
        PG_ATTRIBUTE_TYPE_OID(75, "pg_attribute", TypType.COMPOSITE),
        PG_PROC_TYPE_OID(81, "pg_proc", TypType.COMPOSITE),
        PG_CLASS_TYPE_OID(83, "pg_class", TypType.COMPOSITE),
        JSON_TYPE_OID(114, "json", BinaryEncoding.STRING_BYTES, Types.VARCHAR),
        XML_TYPE_OID(142, "xml"),
        _XML_TYPE_OID(143, "_xml"),
        _JSON_TYPE_OID(199, "_json"),
        SMGR_TYPE_OID(210, "smgr"),
        POINT_TYPE_OID(600, "point"),
        LSEG_TYPE_OID(601, "lseg"),
        PATH_TYPE_OID(602, "path"),
        BOX_TYPE_OID(603, "box"),
        POLYGON_TYPE_OID(604, "polygon"),
        LINE_TYPE_OID(628, "line"),
        _LINE_TYPE_OID(629, "_line"),
        FLOAT4_TYPE_OID(700, "float4", BinaryEncoding.FLOAT_32, Types.REAL),
        FLOAT8_TYPE_OID(701, "float8", BinaryEncoding.FLOAT_64, Types.DOUBLE),
        ABSTIME_TYPE_OID(702, "abstime"),
        RELTIME_TYPE_OID(703, "reltime"),
        TINTERVAL_TYPE_OID(704, "tinterval"),
        UNKNOWN_TYPE_OID(705, "unknown"),
        CIRCLE_TYPE_OID(718, "circle"),
        _CIRCLE_TYPE_OID(719, "_circle"),
        MONEY_TYPE_OID(790, "money"),
        _MONEY_TYPE_OID(791, "_money"),
        MACADDR_TYPE_OID(829, "macaddr"),
        INET_TYPE_OID(869, "inet"),
        CIDR_TYPE_OID(650, "cidr"),
        _BOOL_TYPE_OID(1000, "_bool"),
        _BYTEA_TYPE_OID(1001, "_bytea"),
        _CHAR_TYPE_OID(1002, "_char"),
        _NAME_TYPE_OID(1003, "_name"),
        _INT2_TYPE_OID(1005, "_int2"),
        _INT2VECTOR_TYPE_OID(1006, "_int2vector"),
        _INT4_TYPE_OID(1007, "_int4"),
        _REGPROC_TYPE_OID(1008, "_regproc"),
        _TEXT_TYPE_OID(1009, "_text"),
        _OID_TYPE_OID(1028, "_oid"),
        _TID_TYPE_OID(1010, "_tid"),
        _XID_TYPE_OID(1011, "_xid"),
        _CID_TYPE_OID(1012, "_cid"),
        _OIDVECTOR_TYPE_OID(1013, "_oidvector"),
        _BPCHAR_TYPE_OID(1014, "_bpchar"),
        _VARCHAR_TYPE_OID(1015, "_varchar"),
        _INT8_TYPE_OID(1016, "_int8"),
        _POINT_TYPE_OID(1017, "_point"),
        _LSEG_TYPE_OID(1018, "_lseg"),
        _PATH_TYPE_OID(1019, "_path"),
        _BOX_TYPE_OID(1020, "_box"),
        _FLOAT4_TYPE_OID(1021, "_float4"),
        _FLOAT8_TYPE_OID(1022, "_float8"),
        _ABSTIME_TYPE_OID(1023, "_abstime"),
        _RELTIME_TYPE_OID(1024, "_reltime"),
        _TINTERVAL_TYPE_OID(1025, "_tinterval"),
        _POLYGON_TYPE_OID(1027, "_polygon"),
        ACLITEM_TYPE_OID(1033, "_aclitem"),
        _ACLITEM_TYPE_OID(1034, "_aclitem"),
        _MACADDR_TYPE_OID(1040, "_macaddr"),
        _INET_TYPE_OID(1041, "_inet"),
        _CIDR_TYPE_OID(651, "_cidr"),
        _CSTRING_TYPE_OID(1263, "_cstring"),
        BPCHAR_TYPE_OID(1042, "bpchar", BinaryEncoding.STRING_BYTES, Types.VARCHAR),
        VARCHAR_TYPE_OID(1043, "varchar", BinaryEncoding.STRING_BYTES, Types.VARCHAR),
        DATE_TYPE_OID(1082, "date", BinaryEncoding.DAYS_2000, Types.DATE),
        TIME_TYPE_OID(1083, "time", BinaryEncoding.TIME_INT64_MICROS_NOTZ, Types.TIME),
        TIMESTAMP_TYPE_OID(1114, "timestamp", BinaryEncoding.TIMESTAMP_INT64_MICROS_2000_NOTZ, Types.TIMESTAMP),
        _TIMESTAMP_TYPE_OID(1115, "_timestamp"),
        _DATE_TYPE_OID(1182, "_date"),
        _TIME_TYPE_OID(1183, "_time"),
        TIMESTAMPTZ_TYPE_OID(1184, "timestamptz"),
        _TIMESTAMPTZ_TYPE_OID(1185, "_timestamptz"),
        INTERVAL_TYPE_OID(1186, "interval"),
        _INTERVAL_TYPE_OID(1187, "_interval"),
        _NUMERIC_TYPE_OID(1231, "_numeric"),
        TIMETZ_TYPE_OID(1266, "timetz"),
        _TIMETZ_TYPE_OID(1270, "_timetz"),
        BIT_TYPE_OID(1560, "bit"),
        _BIT_TYPE_OID(1561, "_bit"),
        VARBIT_TYPE_OID(1562, "varbit"),
        _VARBIT_TYPE_OID(1563, "_varbit"),
        NUMERIC_TYPE_OID(1700, "numeric", BinaryEncoding.DECIMAL_PG_NUMERIC_VAR, Types.DECIMAL),
        REFCURSOR_TYPE_OID(1790, "refcursor"),
        _REFCURSOR_TYPE_OID(2201, "_refcursor"),
        REGPROCEDURE_TYPE_OID(2202, "regprocedure"),
        REGOPER_TYPE_OID(2203, "regoper"),
        REGOPERATOR_TYPE_OID(2204, "regoperator"),
        UUID_TYPE_OID(2950, "uuid", BinaryEncoding.UUID, Types.OTHER),
        _UUID_TYPE_OID(2951, "_uuid", BinaryEncoding.UUID, Types.OTHER);
        
        enum TypType {
            BASE,
            COMPOSITE,
            DOMAIN,
            ENUM,
            PSEUDO;
        }

        private int oid;
        private String name;
        private TypType type;
        private BinaryEncoding binaryEncoding;
        private int jdbcType;
        
        TypeOid(int oid, String name, TypType type, 
                BinaryEncoding binaryEncoding, int jdbcType) {
            this.oid = oid;
            this.name = name;
            this.type = type;
            this.binaryEncoding = binaryEncoding;
            this.jdbcType = jdbcType;
        }

        TypeOid(int oid, String name, TypType type) {
            this(oid, name, type, BinaryEncoding.NONE, Types.OTHER);
        }

        TypeOid(int oid, String name, BinaryEncoding binaryEncoding, int jdbcType) {
            this(oid, name, TypType.BASE, binaryEncoding, jdbcType);
        }

        TypeOid(int oid, String name) {
            this(oid, name, TypType.BASE, BinaryEncoding.NONE, Types.OTHER);
        }

        public int getOid() {
            return oid;
        }

        public String getName() {
            return name;
        }

        public TypType getType() {
            return type;
        }

        public BinaryEncoding getBinaryEncoding() {
            return binaryEncoding;
        }

        public int getJDBCType() {
            return jdbcType;
        }

        public static TypeOid fromOid(int oid) {
            for (TypeOid inst : values()) {
                if (inst.getOid() == oid) {
                    return inst;
                }
            }
            return null;
        }

    }
    
    /*** Representation. ***/
    private TypeOid oid;
    private short length;
    private int modifier;

    public PostgresType(TypeOid oid, short length, int modifier, TInstance type) {
        super(type);
        this.oid = oid;
        this.length = length;
        this.modifier = modifier;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return oid.getBinaryEncoding();
    }    

    public int getOid() {
        return oid.getOid();
    }
    public String getTypeName() {
        return oid.getName();
    }
    public short getLength() {
        return length;
    }
    public int getModifier() {
        return modifier;
    }

    public static PostgresType fromAIS(Column aisColumn) {
        return fromTInstance(aisColumn.getType());
    }
        
    public static PostgresType fromAIS(Parameter aisParameter) {
        return fromTInstance(aisParameter.getType());
    }
        
    public static PostgresType fromTInstance(TInstance type)  {
        TClass tClass = TInstance.tClass(type);
        
        TypeOid oid;
        switch (tClass.jdbcType()) {
        case Types.CHAR:
        case Types.NCHAR:
            /* TODO: Should be:
            oid = TypeOid.CHAR_TYPE_OID;
            break;
            */
        case Types.NVARCHAR:
        case Types.VARCHAR:
            oid = TypeOid.VARCHAR_TYPE_OID;
            break;
        case Types.BOOLEAN:
            oid = TypeOid.BOOL_TYPE_OID;
            break;
        case Types.TINYINT:
            oid = TypeOid.INT2_TYPE_OID; // No INT1
            break;
        case Types.SMALLINT:
            if (tClass.isUnsigned())
                oid = TypeOid.INT4_TYPE_OID;
            else
                oid = TypeOid.INT2_TYPE_OID;
            break;
        case Types.INTEGER:
            if (tClass.isUnsigned())
                oid = TypeOid.INT8_TYPE_OID;
            else
                oid = TypeOid.INT4_TYPE_OID;
            break;
        case Types.BIGINT:
            if (tClass.isUnsigned())
                // Closest exact numeric type capable of holding 64-bit unsigned is DEC(20).
                return new PostgresType(TypeOid.NUMERIC_TYPE_OID, (short)8, (20 << 16) + 4,
                        type);
            else
                oid = TypeOid.INT8_TYPE_OID;
            break;
        case Types.DECIMAL:
        case Types.NUMERIC:
            oid = TypeOid.NUMERIC_TYPE_OID;
            break;
        case Types.FLOAT:
        case Types.REAL:
            oid = TypeOid.FLOAT4_TYPE_OID;
            break;
        case Types.DOUBLE:
            oid = TypeOid.FLOAT8_TYPE_OID;
            break;
        case Types.DATE:
            oid = TypeOid.DATE_TYPE_OID;
            break;
        case Types.TIME:
            oid = TypeOid.TIME_TYPE_OID;
            break;
        case Types.TIMESTAMP:
            oid = TypeOid.TIMESTAMP_TYPE_OID;
            break;
        case Types.BINARY:
        case Types.BIT:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            oid = TypeOid.BYTEA_TYPE_OID;
            break;
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            oid = TypeOid.TEXT_TYPE_OID;
            break;
        case Types.OTHER:
            if (tClass == AkGUID.INSTANCE){
                oid = TypeOid.UUID_TYPE_OID;
                break;
            }
            default:
            // Tell Postgres layer to just parse / format a string.
            oid = TypeOid.VARCHAR_TYPE_OID;
            break;
        }
        
        short length = -1;
        int modifier = -1;

        if (tClass.hasFixedSerializationSize())
            length = (short)tClass.fixedSerializationSize();

        if (type.hasAttributes(StringAttribute.class)) {
            // VARCHAR(n).
            modifier = type.attribute(StringAttribute.MAX_LENGTH) + 4;
        }
        else if (type.hasAttributes(TBinary.Attrs.class)) {
            // VARBINARY(n).
            modifier = type.attribute(TBinary.Attrs.LENGTH) + 4;
        }
        else if (type.hasAttributes(DecimalAttribute.class)) {
            // NUMERIC(n,m).
            modifier = (type.attribute(DecimalAttribute.PRECISION) << 16) +
                type.attribute(DecimalAttribute.SCALE) + 4;
        }

        return new PostgresType(oid, length, modifier, type);
    }

    public static PostgresType fromDerby(DataTypeDescriptor sqlType, final TInstance type)  {
        TypeOid oid;
        short length = -1;
        int modifier = -1;

        TypeId typeId = sqlType.getTypeId();
        
        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            oid = TypeOid.INTERVAL_TYPE_OID;
            break;
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            oid = TypeOid.INTERVAL_TYPE_OID;
            break;
        case TypeId.FormatIds.BIT_TYPE_ID:
            oid = TypeOid.BYTEA_TYPE_OID;
            break;
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            oid = TypeOid.BOOL_TYPE_OID;
            break;
        case TypeId.FormatIds.CHAR_TYPE_ID:
            oid = TypeOid.BPCHAR_TYPE_OID;
            break;
        case TypeId.FormatIds.DATE_TYPE_ID:
            oid = TypeOid.DATE_TYPE_OID;
            break;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            oid = TypeOid.NUMERIC_TYPE_OID;
            break;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            oid = TypeOid.FLOAT8_TYPE_OID;
            break;
        case TypeId.FormatIds.INT_TYPE_ID:
            if (typeId.isUnsigned())
                oid = TypeOid.INT8_TYPE_OID;
            else
                oid = TypeOid.INT4_TYPE_OID;
            break;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                return new PostgresType(TypeOid.NUMERIC_TYPE_OID, (short)8, (20 << 16) + 4,
                        type);
            }
            oid = TypeOid.INT8_TYPE_OID;
            break;
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.REAL_TYPE_ID:
            oid = TypeOid.FLOAT4_TYPE_OID;
            break;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            if (typeId.isUnsigned())
                oid = TypeOid.INT4_TYPE_OID;
            else
                oid = TypeOid.INT2_TYPE_OID;
            break;
        case TypeId.FormatIds.TIME_TYPE_ID:
            oid = TypeOid.TIME_TYPE_OID;
            break;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            // TODO: MDatetimes.TIMESTAMP is MYSQL_TIMESTAMP, another
            // way of representing seconds precision, not ISO
            // timestamp with fractional seconds.
            oid = TypeOid.TIMESTAMP_TYPE_OID;
            break;
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            oid = TypeOid.INT2_TYPE_OID; // No INT1, room for unsigned
            break;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            oid = TypeOid.BYTEA_TYPE_OID;
            break;
        case TypeId.FormatIds.BLOB_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            oid = TypeOid.VARCHAR_TYPE_OID;
            break;
        case TypeId.FormatIds.CLOB_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            break;
        case TypeId.FormatIds.XML_TYPE_ID:
            oid = TypeOid.XML_TYPE_OID;
            break;
        case TypeId.FormatIds.GUID_TYPE_ID:
            oid = TypeOid.UUID_TYPE_OID;
            break;
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
        default:
            throw new UnknownDataTypeException(sqlType.toString());
        }

        if (typeId.isDecimalTypeId() || typeId.isNumericTypeId()) {
            modifier = (sqlType.getPrecision() << 16) + sqlType.getScale() + 4;
        }
        else if (typeId.variableLength()) {
            modifier = sqlType.getMaximumWidth() + 4;
        }
        else {
            length = (short)typeId.getMaximumMaximumWidth();
        }

        return new PostgresType(oid, length, modifier, type);
    }

    public static int toJDBC(int oid) {
        TypeOid typeOid = TypeOid.fromOid(oid);
        if (typeOid == null)
            return Types.OTHER;
        else
            return typeOid.getJDBCType();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        if (length >= 0) {
            str.append("(").append(length);
            if (modifier >= 0)
                str.append(",").append(modifier);
            str.append(")");
        }
        str.append("/").append(oid.getName());
        return str.toString();
    }

}
