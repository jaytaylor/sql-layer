
package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TStrongCasts;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

public final class Strongs {

    public static final TStrongCasts fromStrings = TStrongCasts.
            from(
                MString.VARCHAR, MString.CHAR, MString.TINYTEXT, MString.TEXT, MString.MEDIUMTEXT, MString.LONGTEXT,
                MBinary.VARBINARY, MBinary.BINARY, MBinary.TINYBLOB, MBinary.BLOB, MBinary.MEDIUMBLOB, MBinary.LONGBLOB)
            .to(MDatetimes.DATE,
                MDatetimes.DATETIME,
                MDatetimes.TIME,
                MDatetimes.TIMESTAMP,
                MDatetimes.YEAR,
                MApproximateNumber.DOUBLE);

    public static final TStrongCasts textsToVarchar = TStrongCasts
            .from(MString.CHAR, MString.TINYTEXT, MString.TEXT, MString.MEDIUMTEXT, MString.LONGTEXT)
            .to(MString.VARCHAR);

    public static final TStrongCasts blobsToBinary = TStrongCasts
            .from(MBinary.BINARY, MBinary.TINYBLOB, MBinary.BLOB, MBinary.MEDIUMBLOB, MBinary.LONGBLOB)
            .to(MBinary.VARBINARY);

    public static final TStrongCasts charsToBinaries = TStrongCasts
            .from(MString.VARCHAR, MString.CHAR, MString.TINYTEXT, MString.TEXT, MString.MEDIUMTEXT, MString.LONGTEXT)
            .to(MBinary.VARBINARY, MBinary.BINARY, MBinary.TINYBLOB, MBinary.BLOB, MBinary.MEDIUMBLOB, MBinary.LONGBLOB);

    public static final TStrongCasts fromChar = TStrongCasts.from(MString.CHAR).to(
            MString.TINYTEXT,
            MString.TEXT,
            MString.MEDIUMTEXT,
            MString.LONGTEXT
    );

    public static final TStrongCasts fromTinytext = TStrongCasts.from(MString.TINYTEXT).to(
            MString.TEXT,
            MString.MEDIUMTEXT,
            MString.LONGTEXT
    );

    public static final TStrongCasts fromText = TStrongCasts.from(MString.TEXT).to(
            MString.MEDIUMTEXT,
            MString.LONGTEXT
    );

    public static final TStrongCasts fromMediumtext = TStrongCasts.from(MString.MEDIUMTEXT).to(
            MString.LONGTEXT
    );

    public static final TStrongCasts fromBinary = TStrongCasts.from(MBinary.BINARY).to(
            MBinary.TINYBLOB,
            MBinary.BLOB,
            MBinary.MEDIUMBLOB,
            MBinary.LONGBLOB
    );

    public static final TStrongCasts fromTinyblob = TStrongCasts.from(MBinary.TINYBLOB).to(
            MBinary.BLOB,
            MBinary.MEDIUMBLOB,
            MBinary.LONGBLOB
    );

    public static final TStrongCasts fromBlob = TStrongCasts.from(MBinary.BLOB).to(
            MBinary.MEDIUMBLOB,
            MBinary.LONGBLOB
    );

    public static final TStrongCasts fromMediumblob = TStrongCasts.from(MBinary.MEDIUMBLOB).to(
            MBinary.LONGBLOB
    );

    public static final TStrongCasts fromTinyint = TStrongCasts.from(MNumeric.TINYINT).to(
            MNumeric.SMALLINT,
            MNumeric.MEDIUMINT,
            MNumeric.INT,
            MNumeric.BIGINT,
            MNumeric.SMALLINT_UNSIGNED,
            MNumeric.MEDIUMINT_UNSIGNED,
            MNumeric.INT_UNSIGNED,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromSmallint = TStrongCasts.from(MNumeric.SMALLINT).to(
            MNumeric.MEDIUMINT,
            MNumeric.INT,
            MNumeric.BIGINT,
            MNumeric.MEDIUMINT_UNSIGNED,
            MNumeric.INT_UNSIGNED,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromMediumint = TStrongCasts.from(MNumeric.MEDIUMINT).to(
            MNumeric.INT,
            MNumeric.BIGINT,
            MNumeric.INT_UNSIGNED,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromInt = TStrongCasts.from(MNumeric.INT).to(
            MNumeric.BIGINT,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromBigint = TStrongCasts.from(MNumeric.BIGINT).to(
            MNumeric.DECIMAL,
            MApproximateNumber.DOUBLE
    );


    public static final TStrongCasts fromTinyintUnsigned = TStrongCasts.from(MNumeric.TINYINT_UNSIGNED).to(
            MNumeric.SMALLINT_UNSIGNED,
            MNumeric.MEDIUMINT_UNSIGNED,
            MNumeric.INT_UNSIGNED,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromSmallintUnsigned = TStrongCasts.from(MNumeric.SMALLINT_UNSIGNED).to(
            MNumeric.MEDIUMINT_UNSIGNED,
            MNumeric.INT_UNSIGNED,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromMediumintUnsigned = TStrongCasts.from(MNumeric.MEDIUMINT_UNSIGNED).to(
            MNumeric.INT_UNSIGNED,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromIntUnsigned = TStrongCasts.from(MNumeric.INT_UNSIGNED).to(
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,
            MApproximateNumber.DOUBLE
    );
    public static final TStrongCasts fromBigintUnsigned = TStrongCasts.from(MNumeric.BIGINT_UNSIGNED).to(
            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromDatetime = TStrongCasts.from(MDatetimes.DATETIME).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromDate = TStrongCasts.from(MDatetimes.DATE).to(
            MDatetimes.DATETIME,
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromTime = TStrongCasts.from(MDatetimes.TIME).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromYear = TStrongCasts.from(MDatetimes.YEAR).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromTimestamp = TStrongCasts.from(MDatetimes.TIMESTAMP).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromDecimal = TStrongCasts.from(MNumeric.DECIMAL).to(
            MNumeric.DECIMAL_UNSIGNED,
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromDecimalUnsigned = TStrongCasts.from(MNumeric.DECIMAL_UNSIGNED).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromFloat = TStrongCasts.from(MApproximateNumber.FLOAT).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromFloatUnsigned = TStrongCasts.from(MApproximateNumber.FLOAT_UNSIGNED).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromDoubleUnsigned = TStrongCasts.from(MApproximateNumber.DOUBLE_UNSIGNED).to(
            MApproximateNumber.DOUBLE
    );

    // Not generally loss-less, but MySQL allows anything to be cast to bool (e.g. WHERE <type>)
    public static final TStrongCasts toBoolean = TStrongCasts.from(
            MApproximateNumber.FLOAT,
            MApproximateNumber.FLOAT_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MApproximateNumber.DOUBLE_UNSIGNED,
            MBinary.VARBINARY,
            MBinary.BINARY,
            MBinary.TINYBLOB,
            MBinary.MEDIUMBLOB,
            MBinary.BLOB,
            MBinary.LONGBLOB,
            MDatetimes.DATE,
            MDatetimes.DATETIME,
            MDatetimes.TIME,
            MDatetimes.YEAR,
            MDatetimes.TIMESTAMP,
            MNumeric.TINYINT,
            MNumeric.TINYINT_UNSIGNED,
            MNumeric.SMALLINT,
            MNumeric.SMALLINT_UNSIGNED,
            MNumeric.MEDIUMINT,
            MNumeric.MEDIUMINT_UNSIGNED,
            MNumeric.INT,
            MNumeric.INT_UNSIGNED,
            MNumeric.BIGINT,
            MNumeric.BIGINT_UNSIGNED,
            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,
            MString.CHAR,
            MString.VARCHAR,
            MString.TINYTEXT,
            MString.MEDIUMTEXT,
            MString.TEXT,
            MString.LONGTEXT
    ).to(
            AkBool.INSTANCE
    );
}
