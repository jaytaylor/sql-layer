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

import com.akiban.sql.server.ServerType;

import com.akiban.server.error.UnknownDataTypeException;
import com.akiban.server.error.UnknownTypeSizeException;
import com.akiban.server.types.AkType;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;

/** A type according to the PostgreSQL regime.
 * Information corresponds more-or-less directly to what's in the 
 * <code>pg_attribute</code> table.
 */
public class PostgresType extends ServerType
{
    /*** Type OIDs ***/

    public enum TypeOid {

        BOOL_TYPE_OID(16, "bool", TypType.BASE),
        BYTEA_TYPE_OID(17, "bytea", TypType.BASE),
        CHAR_TYPE_OID(18, "char", TypType.BASE),
        NAME_TYPE_OID(19, "name", TypType.BASE),
        INT8_TYPE_OID(20, "int8", TypType.BASE),
        INT2_TYPE_OID(21, "int2", TypType.BASE),
        INT2VECTOR_TYPE_OID(22, "int2vector", TypType.BASE),
        INT4_TYPE_OID(23, "int4", TypType.BASE),
        REGPROC_TYPE_OID(24, "regproc", TypType.BASE),
        TEXT_TYPE_OID(25, "text", TypType.BASE),
        OID_TYPE_OID(26, "oid", TypType.BASE),
        TID_TYPE_OID(27, "tid", TypType.BASE),
        XID_TYPE_OID(28, "xid", TypType.BASE),
        CID_TYPE_OID(29, "cid", TypType.BASE),
        OIDVECTOR_TYPE_OID(30, "oidvector", TypType.BASE),
        PG_TYPE_TYPE_OID(71, "pg_type", TypType.COMPOSITE),
        PG_ATTRIBUTE_TYPE_OID(75, "pg_attribute", TypType.COMPOSITE),
        PG_PROC_TYPE_OID(81, "pg_proc", TypType.COMPOSITE),
        PG_CLASS_TYPE_OID(83, "pg_class", TypType.COMPOSITE),
        XML_TYPE_OID(142, "xml", TypType.BASE),
        _XML_TYPE_OID(143, "_xml", TypType.BASE),
        SMGR_TYPE_OID(210, "smgr", TypType.BASE),
        POINT_TYPE_OID(600, "point", TypType.BASE),
        LSEG_TYPE_OID(601, "lseg", TypType.BASE),
        PATH_TYPE_OID(602, "path", TypType.BASE),
        BOX_TYPE_OID(603, "box", TypType.BASE),
        POLYGON_TYPE_OID(604, "polygon", TypType.BASE),
        LINE_TYPE_OID(628, "line", TypType.BASE),
        _LINE_TYPE_OID(629, "_line", TypType.BASE),
        FLOAT4_TYPE_OID(700, "float4", TypType.BASE),
        FLOAT8_TYPE_OID(701, "float8", TypType.BASE),
        ABSTIME_TYPE_OID(702, "abstime", TypType.BASE),
        RELTIME_TYPE_OID(703, "reltime", TypType.BASE),
        TINTERVAL_TYPE_OID(704, "tinterval", TypType.BASE),
        UNKNOWN_TYPE_OID(705, "unknown", TypType.BASE),
        CIRCLE_TYPE_OID(718, "circle", TypType.BASE),
        _CIRCLE_TYPE_OID(719, "_circle", TypType.BASE),
        MONEY_TYPE_OID(790, "money", TypType.BASE),
        _MONEY_TYPE_OID(791, "_money", TypType.BASE),
        MACADDR_TYPE_OID(829, "macaddr", TypType.BASE),
        INET_TYPE_OID(869, "inet", TypType.BASE),
        CIDR_TYPE_OID(650, "cidr", TypType.BASE),
        _BOOL_TYPE_OID(1000, "_bool", TypType.BASE),
        _BYTEA_TYPE_OID(1001, "_bytea", TypType.BASE),
        _CHAR_TYPE_OID(1002, "_char", TypType.BASE),
        _NAME_TYPE_OID(1003, "_name", TypType.BASE),
        _INT2_TYPE_OID(1005, "_int2", TypType.BASE),
        _INT2VECTOR_TYPE_OID(1006, "_int2vector", TypType.BASE),
        _INT4_TYPE_OID(1007, "_int4", TypType.BASE),
        _REGPROC_TYPE_OID(1008, "_regproc", TypType.BASE),
        _TEXT_TYPE_OID(1009, "_text", TypType.BASE),
        _OID_TYPE_OID(1028, "_oid", TypType.BASE),
        _TID_TYPE_OID(1010, "_tid", TypType.BASE),
        _XID_TYPE_OID(1011, "_xid", TypType.BASE),
        _CID_TYPE_OID(1012, "_cid", TypType.BASE),
        _OIDVECTOR_TYPE_OID(1013, "_oidvector", TypType.BASE),
        _BPCHAR_TYPE_OID(1014, "_bpchar", TypType.BASE),
        _VARCHAR_TYPE_OID(1015, "_varchar", TypType.BASE),
        _INT8_TYPE_OID(1016, "_int8", TypType.BASE),
        _POINT_TYPE_OID(1017, "_point", TypType.BASE),
        _LSEG_TYPE_OID(1018, "_lseg", TypType.BASE),
        _PATH_TYPE_OID(1019, "_path", TypType.BASE),
        _BOX_TYPE_OID(1020, "_box", TypType.BASE),
        _FLOAT4_TYPE_OID(1021, "_float4", TypType.BASE),
        _FLOAT8_TYPE_OID(1022, "_float8", TypType.BASE),
        _ABSTIME_TYPE_OID(1023, "_abstime", TypType.BASE),
        _RELTIME_TYPE_OID(1024, "_reltime", TypType.BASE),
        _TINTERVAL_TYPE_OID(1025, "_tinterval", TypType.BASE),
        _POLYGON_TYPE_OID(1027, "_polygon", TypType.BASE),
        ACLITEM_TYPE_OID(1033, "_aclitem", TypType.BASE),
        _ACLITEM_TYPE_OID(1034, "_aclitem", TypType.BASE),
        _MACADDR_TYPE_OID(1040, "_macaddr", TypType.BASE),
        _INET_TYPE_OID(1041, "_inet", TypType.BASE),
        _CIDR_TYPE_OID(651, "_cidr", TypType.BASE),
        _CSTRING_TYPE_OID(1263, "_cstring", TypType.BASE),
        BPCHAR_TYPE_OID(1042, "bpchar", TypType.BASE),
        VARCHAR_TYPE_OID(1043, "varchar", TypType.BASE),
        DATE_TYPE_OID(1082, "date", TypType.BASE),
        TIME_TYPE_OID(1083, "time", TypType.BASE),
        TIMESTAMP_TYPE_OID(1114, "timestamp", TypType.BASE),
        _TIMESTAMP_TYPE_OID(1115, "_timestamp", TypType.BASE),
        _DATE_TYPE_OID(1182, "_date", TypType.BASE),
        _TIME_TYPE_OID(1183, "_time", TypType.BASE),
        TIMESTAMPTZ_TYPE_OID(1184, "timestamptz", TypType.BASE),
        _TIMESTAMPTZ_TYPE_OID(1185, "_timestamptz", TypType.BASE),
        INTERVAL_TYPE_OID(1186, "interval", TypType.BASE),
        _INTERVAL_TYPE_OID(1187, "_interval", TypType.BASE),
        _NUMERIC_TYPE_OID(1231, "_numeric", TypType.BASE),
        TIMETZ_TYPE_OID(1266, "timetz", TypType.BASE),
        _TIMETZ_TYPE_OID(1270, "_timetz", TypType.BASE),
        BIT_TYPE_OID(1560, "bit", TypType.BASE),
        _BIT_TYPE_OID(1561, "_bit", TypType.BASE),
        VARBIT_TYPE_OID(1562, "varbit", TypType.BASE),
        _VARBIT_TYPE_OID(1563, "_varbit", TypType.BASE),
        NUMERIC_TYPE_OID(1700, "numeric", TypType.BASE),
        REFCURSOR_TYPE_OID(1790, "refcursor", TypType.BASE),
        _REFCURSOR_TYPE_OID(2201, "_refcursor", TypType.BASE),
        REGPROCEDURE_TYPE_OID(2202, "regprocedure", TypType.BASE),
        REGOPER_TYPE_OID(2203, "regoper", TypType.BASE),
        REGOPERATOR_TYPE_OID(2204, "regoperator", TypType.BASE);
        
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
        
        TypeOid(int oid, String name, TypType type) {
            this.oid = oid;
            this.name = name;
            this.type = type;
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
    private int oid;
    private short length;
    private int modifier;

    public PostgresType(int oid, short length, int modifier, AkType akType, TInstance instance) {
        super(akType, instance);
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
            oid = TypeOid.VARCHAR_TYPE_OID.getOid();
        else if ("INT".equals(encoding) ||
                 "U_INT".equals(encoding)) {
            switch (aisType.maxSizeBytes().intValue()) {
            case 1:
                oid = TypeOid.INT2_TYPE_OID.getOid(); // No INT1; this also could be BOOLEAN (TINYINT(1)).
                break;
            case 2:
            case 3:
                oid = TypeOid.INT2_TYPE_OID.getOid();
                break;
            case 4:
                oid = TypeOid.INT4_TYPE_OID.getOid();
                break;
            case 8:
                oid = TypeOid.INT8_TYPE_OID.getOid();
                break;
            default:
                throw new UnknownTypeSizeException (aisType);
            }
        }
        else if ("U_BIGINT".equals(encoding)) {
            // Closest exact numeric type capable of holding 64-bit unsigned is DEC(20).
            return new PostgresType(TypeOid.NUMERIC_TYPE_OID.getOid(), (short)8, (20 << 16) + 4,
                                    aisType.akType(), aisColumn.tInstance());
        }
        else if ("DATE".equals(encoding))
            oid = TypeOid.DATE_TYPE_OID.getOid();
        else if ("TIME".equals(encoding))
            oid = TypeOid.TIME_TYPE_OID.getOid();
        else if ("DATETIME".equals(encoding) ||
                 "TIMESTAMP".equals(encoding))
            oid = TypeOid.TIMESTAMP_TYPE_OID.getOid();
        else if ("BLOB".equals(encoding) ||
                 "TEXT".equals(encoding))
            oid = TypeOid.TEXT_TYPE_OID.getOid();
        else if ("YEAR".equals(encoding))
            oid = TypeOid.INT2_TYPE_OID.getOid(); // No INT1
        else if ("DECIMAL".equals(encoding) ||
                 "U_DECIMAL".equals(encoding))
            oid = TypeOid.NUMERIC_TYPE_OID.getOid();
        else if ("FLOAT".equals(encoding) ||
                 "U_FLOAT".equals(encoding))
            oid = TypeOid.FLOAT4_TYPE_OID.getOid();
        else if ("DOUBLE".equals(encoding) ||
                 "U_DOUBLE".equals(encoding))
            oid = TypeOid.FLOAT8_TYPE_OID.getOid();
        else if ("VARBINARY".equals(encoding))
            oid = TypeOid.BYTEA_TYPE_OID.getOid();
        else
            throw new UnknownDataTypeException (encoding);

        if (aisType.fixedSize())
            length = aisType.maxSizeBytes().shortValue();

        if (aisColumn != null) {
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
        }

        return new PostgresType(oid, length, modifier, aisType.akType(), aisColumn.tInstance());
    }

    public static PostgresType fromDerby(DataTypeDescriptor type)  {
        int oid;
        short length = -1;
        int modifier = -1;

        TypeId typeId = type.getTypeId();

        AkType akType;
        
        // TODO: replace AkType with Instance
        TInstance instance = null;

        switch (typeId.getTypeFormatId()) {
        case TypeId.FormatIds.INTERVAL_DAY_SECOND_ID:
            oid = TypeOid.INTERVAL_TYPE_OID.getOid();
            akType = AkType.INTERVAL_MILLIS;
            break;
        case TypeId.FormatIds.INTERVAL_YEAR_MONTH_ID:
            oid = TypeOid.INTERVAL_TYPE_OID.getOid();
            akType = AkType.INTERVAL_MONTH;
            break;
        case TypeId.FormatIds.BIT_TYPE_ID:
            oid = TypeOid.BYTEA_TYPE_OID.getOid();
            akType = AkType.VARBINARY;
            break;
        case TypeId.FormatIds.BOOLEAN_TYPE_ID:
            oid = TypeOid.BOOL_TYPE_OID.getOid();
            akType = AkType.BOOL;
            break;
        case TypeId.FormatIds.CHAR_TYPE_ID:
            oid = TypeOid.CHAR_TYPE_OID.getOid();
            akType = AkType.VARCHAR;
            break;
        case TypeId.FormatIds.DATE_TYPE_ID:
            oid = TypeOid.DATE_TYPE_OID.getOid();
            akType = AkType.DATE;
            break;
        case TypeId.FormatIds.DECIMAL_TYPE_ID:
        case TypeId.FormatIds.NUMERIC_TYPE_ID:
            oid = TypeOid.NUMERIC_TYPE_OID.getOid();
            akType = AkType.DECIMAL;
            break;
        case TypeId.FormatIds.DOUBLE_TYPE_ID:
            oid = TypeOid.FLOAT8_TYPE_OID.getOid();
            if (typeId.isUnsigned())
                akType = AkType.U_DOUBLE;
            else
                akType = AkType.DOUBLE;
            break;
        case TypeId.FormatIds.INT_TYPE_ID:
            oid = TypeOid.INT4_TYPE_OID.getOid();
            if (typeId.isUnsigned())
                akType = AkType.U_INT;
            else
                akType = AkType.LONG;
            break;
        case TypeId.FormatIds.LONGINT_TYPE_ID:
            if (typeId.isUnsigned())
                return new PostgresType(TypeOid.NUMERIC_TYPE_OID.getOid(), (short)8, (20 << 16) + 4,
                                        AkType.U_BIGINT, MNumeric.BIGINT_UNSIGNED.instance());
            oid = TypeOid.INT8_TYPE_OID.getOid();
            akType = AkType.LONG;
            break;
        case TypeId.FormatIds.LONGVARBIT_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID.getOid();
            akType = AkType.TEXT;
            break;
        case TypeId.FormatIds.LONGVARCHAR_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID.getOid();
            akType = AkType.TEXT;
            break;
        case TypeId.FormatIds.REAL_TYPE_ID:
            oid = TypeOid.FLOAT4_TYPE_OID.getOid();
            if (typeId.isUnsigned())
                akType = AkType.U_FLOAT;
            else
                akType = AkType.FLOAT;
            break;
        case TypeId.FormatIds.SMALLINT_TYPE_ID:
            oid = TypeOid.INT2_TYPE_OID.getOid();
            if (typeId == TypeId.YEAR_ID)
                akType = AkType.YEAR;
            else
                akType = AkType.INT;
            break;
        case TypeId.FormatIds.TIME_TYPE_ID:
            oid = TypeOid.TIME_TYPE_OID.getOid();
            akType = AkType.TIME;
            break;
        case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
            oid = TypeOid.TIMESTAMP_TYPE_OID.getOid();
            if (typeId == TypeId.DATETIME_ID)
                akType = AkType.DATETIME;
            else
                // TODO: AkType.TIMESTAMP is MYSQL_TIMESTAMP, another
                // way of representing seconds precision, not ISO
                // timestamp with fractional seconds.
                akType = AkType.TIMESTAMP;
            break;
        case TypeId.FormatIds.TINYINT_TYPE_ID:
            oid = TypeOid.INT2_TYPE_OID.getOid(); // No INT1
            akType = AkType.INT;
            break;
        case TypeId.FormatIds.VARBIT_TYPE_ID:
            oid = TypeOid.BYTEA_TYPE_OID.getOid();
            akType = AkType.VARBINARY;
            break;
        case TypeId.FormatIds.BLOB_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID.getOid();
            akType = AkType.VARBINARY;
            break;
        case TypeId.FormatIds.VARCHAR_TYPE_ID:
            oid = TypeOid.VARCHAR_TYPE_OID.getOid();
            akType = AkType.VARCHAR;
            break;
        case TypeId.FormatIds.CLOB_TYPE_ID:
            oid = TypeOid.TEXT_TYPE_OID.getOid();
            akType = AkType.TEXT;
            break;
        case TypeId.FormatIds.XML_TYPE_ID:
            oid = TypeOid.XML_TYPE_OID.getOid();
            akType = AkType.TEXT;
            break;
        case TypeId.FormatIds.USERDEFINED_TYPE_ID:
            {
                // Might be a type known to AIS but not to Derby.
                // TODO: Need to reconcile.
                String name = typeId.getSQLTypeName();
                for (Type aisType : Types.types()) {
                    if (aisType.name().equalsIgnoreCase(name)) {
                        return fromAIS(aisType, null);
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
        
        return new PostgresType(oid, length, modifier, akType, instance);
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
        TypeOid inst = TypeOid.fromOid(oid);
        if (inst != null)
            str.append("/").append(inst.getName());
        return str.toString();
    }

}
