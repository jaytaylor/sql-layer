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

package com.akiban.sql.pg;

import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.aksql.aktypes.AkInterval;
import com.akiban.server.types3.common.types.StringFactory.Charset;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.sql.server.ServerType;

import com.akiban.server.error.UnknownDataTypeException;
import com.akiban.server.error.UnknownTypeSizeException;
import com.akiban.server.types.AkType;

import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A type according to the PostgreSQL regime.
 * Information corresponds more-or-less directly to what's in the 
 * <code>pg_attribute</code> table.
 */
public class PostgresType extends ServerType
{

    private static Logger logger = LoggerFactory.getLogger(PostgresType.class);

    /*** Type OIDs ***/

    public enum TypeOid {

        BOOL_TYPE_OID(16, "bool", BinaryEncoding.BOOLEAN_C),
        BYTEA_TYPE_OID(17, "bytea", BinaryEncoding.BINARY_OCTAL_TEXT),
        CHAR_TYPE_OID(18, "char"),
        NAME_TYPE_OID(19, "name"),
        INT8_TYPE_OID(20, "int8", BinaryEncoding.INT_64),
        INT2_TYPE_OID(21, "int2", BinaryEncoding.INT_16),
        INT2VECTOR_TYPE_OID(22, "int2vector"),
        INT4_TYPE_OID(23, "int4", BinaryEncoding.INT_32),
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
        XML_TYPE_OID(142, "xml"),
        _XML_TYPE_OID(143, "_xml"),
        SMGR_TYPE_OID(210, "smgr"),
        POINT_TYPE_OID(600, "point"),
        LSEG_TYPE_OID(601, "lseg"),
        PATH_TYPE_OID(602, "path"),
        BOX_TYPE_OID(603, "box"),
        POLYGON_TYPE_OID(604, "polygon"),
        LINE_TYPE_OID(628, "line"),
        _LINE_TYPE_OID(629, "_line"),
        FLOAT4_TYPE_OID(700, "float4", BinaryEncoding.FLOAT_32),
        FLOAT8_TYPE_OID(701, "float8", BinaryEncoding.FLOAT_64),
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
        BPCHAR_TYPE_OID(1042, "bpchar", BinaryEncoding.STRING_BYTES),
        VARCHAR_TYPE_OID(1043, "varchar", BinaryEncoding.STRING_BYTES),
        DATE_TYPE_OID(1082, "date"),
        TIME_TYPE_OID(1083, "time"),
        TIMESTAMP_TYPE_OID(1114, "timestamp", BinaryEncoding.TIMESTAMP_FLOAT64_SECS_2000_NOTZ),
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
        NUMERIC_TYPE_OID(1700, "numeric", BinaryEncoding.DECIMAL_PG_NUMERIC_VAR),
        REFCURSOR_TYPE_OID(1790, "refcursor"),
        _REFCURSOR_TYPE_OID(2201, "_refcursor"),
        REGPROCEDURE_TYPE_OID(2202, "regprocedure"),
        REGOPER_TYPE_OID(2203, "regoper"),
        REGOPERATOR_TYPE_OID(2204, "regoperator");
        
        enum TypType {
            BASE,
            COMPOSITE,
            DOMAIN,
            ENUM,
            PSEDUO;
        }

        private int oid;
        private String name;
        private TypType type;
        private BinaryEncoding binaryEncoding;
        
        TypeOid(int oid, String name, TypType type, BinaryEncoding binaryEncoding) {
            this.oid = oid;
            this.name = name;
            this.type = type;
            this.binaryEncoding = binaryEncoding;
        }

        TypeOid(int oid, String name, TypType type) {
            this(oid, name, type, BinaryEncoding.NONE);
        }

        TypeOid(int oid, String name, BinaryEncoding binaryEncoding) {
            this(oid, name, TypType.BASE, binaryEncoding);
        }

        TypeOid(int oid, String name) {
            this(oid, name, TypType.BASE, BinaryEncoding.NONE);
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

    public PostgresType(TypeOid oid, short length, int modifier, AkType akType, TInstance instance) {
        super(akType, instance);
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
    public short getLength() {
        return length;
    }
    public int getModifier() {
        return modifier;
    }

    public static PostgresType fromAIS(Column aisColumn) {
        return fromAIS(aisColumn.getType(), aisColumn, aisColumn.getNullable(), null);
    }
        
    public static PostgresType fromAIS(Type aisType, Column aisColumn, boolean nullable, TInstance tInstance)  {
        TypeOid oid;
        short length = -1;
        int modifier = -1;

        String encoding = aisType.encoding();

        if ("VARCHAR".equals(encoding))
            oid = TypeOid.VARCHAR_TYPE_OID;
        else if ("INT".equals(encoding) ||
                 "U_INT".equals(encoding)) {
            switch (aisType.maxSizeBytes().intValue()) {
            case 1:
                oid = TypeOid.INT2_TYPE_OID; // No INT1; this also could be BOOLEAN (TINYINT(1)).
                break;
            case 2:
            case 3:
                oid = TypeOid.INT2_TYPE_OID;
                break;
            case 4:
                oid = TypeOid.INT4_TYPE_OID;
                break;
            case 8:
                oid = TypeOid.INT8_TYPE_OID;
                break;
            default:
                throw new UnknownTypeSizeException (aisType);
            }
        }
        else if ("U_BIGINT".equals(encoding)) {
            // Closest exact numeric type capable of holding 64-bit unsigned is DEC(20).
            return new PostgresType(TypeOid.NUMERIC_TYPE_OID, (short)8, (20 << 16) + 4,
                                    aisType.akType(), MNumeric.BIGINT_UNSIGNED.instance());
        }
        else if ("DATE".equals(encoding))
            oid = TypeOid.DATE_TYPE_OID;
        else if ("TIME".equals(encoding))
            oid = TypeOid.TIME_TYPE_OID;
        else if ("DATETIME".equals(encoding) ||
                 "TIMESTAMP".equals(encoding))
            oid = TypeOid.TIMESTAMP_TYPE_OID;
        else if ("BLOB".equals(encoding) ||
                 "TEXT".equals(encoding))
            oid = TypeOid.TEXT_TYPE_OID;
        else if ("YEAR".equals(encoding))
            oid = TypeOid.INT2_TYPE_OID; // No INT1
        else if ("DECIMAL".equals(encoding) ||
                 "U_DECIMAL".equals(encoding))
            oid = TypeOid.NUMERIC_TYPE_OID;
        else if ("FLOAT".equals(encoding) ||
                 "U_FLOAT".equals(encoding))
            oid = TypeOid.FLOAT4_TYPE_OID;
        else if ("DOUBLE".equals(encoding) ||
                 "U_DOUBLE".equals(encoding))
            oid = TypeOid.FLOAT8_TYPE_OID;
        else if ("VARBINARY".equals(encoding))
            oid = TypeOid.BYTEA_TYPE_OID;
        else
            throw new UnknownDataTypeException (encoding);

        if (aisType.fixedSize())
            length = aisType.maxSizeBytes().shortValue();

        TInstance instance;
        if (tInstance != null) {
            instance = tInstance;
        }
        else if (aisColumn != null) {
            switch (aisType.nTypeParameters()) {
            case 1:
                // VARCHAR(n).
                modifier = aisColumn.getTypeParameter1().intValue() + 4;
                break;
            case 2:
                // NUMERIC(n,m).
                modifier = (aisColumn.getTypeParameter1().intValue() << 16) +
                           aisColumn.getTypeParameter2().intValue() + 4;
                break;
            }
            instance = aisColumn.tInstance();
        }
        else {
            instance = Column.generateTInstance(null, aisType, null, null, nullable);
        }
        return new PostgresType(oid, length, modifier, aisType.akType(), instance);
    }

    public static PostgresType fromDerby(DataTypeDescriptor type, TInstance tInstance)  {
        TypeOid oid;
        short length = -1;
        int modifier = -1;

        TypeId typeId = type.getTypeId();

        final AkType akType;

        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            oid = TypeOid.INTERVAL_TYPE_OID;
            akType = AkType.INTERVAL_MILLIS;
            if (tInstance == null) tInstance = AkInterval.SECONDS.tInstanceFrom(type);
            break;
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            oid = TypeOid.INTERVAL_TYPE_OID;
            akType = AkType.INTERVAL_MONTH;
            if (tInstance == null) tInstance = AkInterval.MONTHS.tInstanceFrom(type);
            break;
        case TypeId.FormatIds.BIT_TYPE_ID:
            oid = TypeOid.BYTEA_TYPE_OID;
            akType = AkType.VARBINARY;
            if (tInstance == null) tInstance = MBinary.VARBINARY.instance(type.getMaximumWidth());
            break;
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            oid = TypeOid.BOOL_TYPE_OID;
            akType = AkType.BOOL;
            if (tInstance == null) tInstance = AkBool.INSTANCE.instance();
            break;
        case TypeId.FormatIds.CHAR_TYPE_ID:
            oid = TypeOid.BPCHAR_TYPE_OID;
            akType = AkType.VARCHAR;
            if (tInstance == null) tInstance = charType(type, MString.VARCHAR);
            break;
        case TypeId.FormatIds.DATE_TYPE_ID:
            oid = TypeOid.DATE_TYPE_OID;
            akType = AkType.DATE;
            if (tInstance == null) tInstance = MDatetimes.DATE.instance();
            break;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            oid = TypeOid.NUMERIC_TYPE_OID;
            akType = AkType.DECIMAL;
            if (tInstance == null) tInstance = MNumeric.DECIMAL.instance(type.getPrecision(), type.getScale());
            break;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            oid = TypeOid.FLOAT8_TYPE_OID;
            if (typeId.isUnsigned()) {
                akType = AkType.U_DOUBLE;
                if (tInstance == null) tInstance = MApproximateNumber.DOUBLE_UNSIGNED.instance();
            }
            else {
                akType = AkType.DOUBLE;
                if (tInstance == null) tInstance = MApproximateNumber.DOUBLE.instance();
            }
            break;
        case TypeId.FormatIds.INT_TYPE_ID:
            oid = TypeOid.INT4_TYPE_OID;
            if (typeId.isUnsigned()) {
                akType = AkType.U_INT;
                if (tInstance == null) tInstance = MNumeric.INT_UNSIGNED.instance();
            }
            else {
                akType = AkType.LONG;
                if (tInstance == null) tInstance = MNumeric.INT.instance();
            }
            break;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned()) {
                if (tInstance == null) tInstance = MNumeric.BIGINT_UNSIGNED.instance();
                return new PostgresType(TypeOid.NUMERIC_TYPE_OID, (short)8, (20 << 16) + 4,
                                        AkType.U_BIGINT, tInstance);
            }
            oid = TypeOid.INT8_TYPE_OID;
            akType = AkType.LONG;
            if (tInstance == null) tInstance = MNumeric.BIGINT.instance();
            break;
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            akType = AkType.TEXT;
            if (tInstance == null) tInstance = charType(type, MString.TEXT);
            break;
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            akType = AkType.TEXT;
            if (tInstance == null) tInstance = charType(type, MString.TEXT);
            break;
        case TypeId.FormatIds.REAL_TYPE_ID:
            oid = TypeOid.FLOAT4_TYPE_OID;
            if (typeId.isUnsigned()) {
                akType = AkType.U_FLOAT;
                if (tInstance == null) tInstance = MApproximateNumber.FLOAT_UNSIGNED.instance();
            }
            else {
                akType = AkType.FLOAT;
                if (tInstance == null) tInstance = MApproximateNumber.FLOAT.instance();
            }
            break;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            oid = TypeOid.INT2_TYPE_OID;
            if (typeId == TypeId.YEAR_ID) {
                akType = AkType.YEAR;
                if (tInstance == null) tInstance = MDatetimes.YEAR.instance();
            }
            else {
                akType = AkType.INT;
                if (tInstance == null) tInstance = MNumeric.INT.instance();
            }
            break;
        case TypeId.FormatIds.TIME_TYPE_ID:
            oid = TypeOid.TIME_TYPE_OID;
            akType = AkType.TIME;
            if (tInstance == null) tInstance = MDatetimes.TIME.instance();
            break;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            oid = TypeOid.TIMESTAMP_TYPE_OID;
            if (typeId == TypeId.DATETIME_ID) {
                akType = AkType.DATETIME;
                if (tInstance == null) tInstance = MDatetimes.DATETIME.instance();
            }
            else {
                // TODO: AkType.TIMESTAMP is MYSQL_TIMESTAMP, another
                // way of representing seconds precision, not ISO
                // timestamp with fractional seconds.
                akType = AkType.TIMESTAMP;
                if (tInstance == null) tInstance = MDatetimes.TIMESTAMP.instance();
            }
            break;
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            oid = TypeOid.INT2_TYPE_OID; // No INT1
            akType = AkType.INT;
            if (tInstance == null) tInstance = MNumeric.INT.instance();
            break;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            oid = TypeOid.BYTEA_TYPE_OID;
            akType = AkType.VARBINARY;
            if (tInstance == null) tInstance = MBinary.VARBINARY.instance(type.getMaximumWidth());
            break;
        case TypeId.FormatIds.BLOB_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            akType = AkType.VARBINARY;
            if (tInstance == null) tInstance = MBinary.VARBINARY.instance(type.getMaximumWidth());
            break;
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            oid = TypeOid.VARCHAR_TYPE_OID;
            akType = AkType.VARCHAR;
            if (tInstance == null) tInstance = charType(type, MString.VARCHAR);
            break;
        case TypeId.FormatIds.CLOB_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID;
            akType = AkType.TEXT;
            if (tInstance == null) tInstance = charType(type, MString.TEXT);
            break;
        case TypeId.FormatIds.XML_TYPE_ID:
            oid = TypeOid.XML_TYPE_OID;
            akType = AkType.TEXT;
            if (tInstance == null) tInstance = charType(type, MString.TEXT);
            break;
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
            {
                // Might be a type known to AIS but not to Derby.
                // TODO: Need to reconcile.
                String name = typeId.getSQLTypeName();
                for (Type aisType : Types.types()) {
                    if (aisType.name().equalsIgnoreCase(name)) {
                        return fromAIS(aisType, null, type.isNullable(), tInstance);
                    }
                }
            }
            /* falls through */
        default:
            throw new UnknownDataTypeException(type.toString());
        }

        if (typeId.isDecimalTypeId() || typeId.isNumericTypeId()) {
            modifier = (type.getPrecision() << 16) + type.getScale() + 4;
        }
        else if (typeId.variableLength()) {
            modifier = type.getMaximumWidth() + 4;
        }
        else {
            length = (short)typeId.getMaximumMaximumWidth();
        }

        if (tInstance == null)
            logger.warn("no TInstance created for {}", type);
        else
            tInstance.setNullable(type.isNullable());
        return new PostgresType(oid, length, modifier, akType, tInstance);
    }

    private static TInstance charType(DataTypeDescriptor type, TString tClass) {
        CharacterTypeAttributes typeAttributes = type.getCharacterAttributes();
        int charsetId = (typeAttributes == null)
                ? -1
                : Charset.of(typeAttributes.getCharacterSet()).ordinal();
        return tClass.instance(type.getMaximumWidth(), charsetId);
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
