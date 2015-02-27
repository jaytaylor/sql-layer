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

package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.types.TStrongCasts;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

public final class Strongs {

    public static final TStrongCasts fromStrings = TStrongCasts.
            from(
                MString.VARCHAR, MString.CHAR, MString.TINYTEXT, MString.TEXT, MString.MEDIUMTEXT, MString.LONGTEXT,
                MBinary.VARBINARY, MBinary.BINARY)
            .to(MDateAndTime.DATE,
                MDateAndTime.DATETIME,
                MDateAndTime.TIME,
                MDateAndTime.TIMESTAMP,
                MDateAndTime.YEAR,
                AkGUID.INSTANCE,
                MApproximateNumber.DOUBLE);

    public static final TStrongCasts textsToVarchar = TStrongCasts
            .from(MString.CHAR, MString.TINYTEXT, MString.TEXT, MString.MEDIUMTEXT, MString.LONGTEXT)
            .to(MString.VARCHAR);

    public static final TStrongCasts blobsToBinary = TStrongCasts
            .from(MBinary.BINARY)
            .to(MBinary.VARBINARY);

    public static final TStrongCasts charsToBinaries = TStrongCasts
            .from(MString.VARCHAR, MString.CHAR, MString.TINYTEXT, MString.TEXT, MString.MEDIUMTEXT, MString.LONGTEXT)
            .to(MBinary.VARBINARY, MBinary.BINARY);

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

    public static final TStrongCasts fromDatetime = TStrongCasts.from(MDateAndTime.DATETIME).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromDate = TStrongCasts.from(MDateAndTime.DATE).to(
            MDateAndTime.DATETIME,
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromTime = TStrongCasts.from(MDateAndTime.TIME).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromYear = TStrongCasts.from(MDateAndTime.YEAR).to(
            MApproximateNumber.DOUBLE
    );

    public static final TStrongCasts fromTimestamp = TStrongCasts.from(MDateAndTime.TIMESTAMP).to(
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
            MDateAndTime.DATE,
            MDateAndTime.DATETIME,
            MDateAndTime.TIME,
            MDateAndTime.YEAR,
            MDateAndTime.TIMESTAMP,
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
