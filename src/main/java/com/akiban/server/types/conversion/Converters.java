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

package com.akiban.server.types.conversion;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import static com.akiban.server.types.AkType.*;

public final class Converters {

    /**
     * Converts the source into the target. This method takes care of all internal conversions between the source's
     * and target's type. For instance, if the source is pointing at a VARCHAR and the target requires a LONG,
     * but the VARCHAR can be parsed into a Long, this method will take care of that parsing for you.
     * @param source the conversion source
     * @param target the conversion target
     * @param <T> the conversion target's specific type
     * @return the conversion target; this return value is provided as a convenience, so you can chain calls
     */
    public static <T extends ValueTarget> T convert(ValueSource source, T target) {
        if (source.isNull()) {
            target.putNull();
        } else {
            AkType conversionType = target.getConversionType();
            if (conversionType == null || conversionType == UNSUPPORTED || conversionType == NULL) {
                throw new InconvertibleTypesException(source.getConversionType(), conversionType);
            }
            get(conversionType).convert(source, target);
        }
        return target;
    }

    public static boolean isConversionAllowed(AkType sourceType, AkType targetType) {
        // NULL -> * is always allowed, UNSUPPORTED -> * is always disallowed, else A -> A is allowed
        if (sourceType == AkType.NULL || (sourceType != AkType.UNSUPPORTED && sourceType.equals(targetType))) {
            return true;
        }
        Set<AkType> allowedTargets = readOnlyLegalConversions.get(sourceType);
        return allowedTargets != null && allowedTargets.contains(targetType);
    }

    // for use in this class
    
    private static AbstractConverter get(AkType type) {
        return readOnlyConvertersMap.get(type);
    }

    private static Map<AkType,AbstractConverter> createConvertersMap() {
        Map<AkType,AbstractConverter> result = new EnumMap<AkType, AbstractConverter>(AkType.class);
        result.put(DATE, LongConverter.DATE);
        result.put(DATETIME, LongConverter.DATETIME);
        result.put(DECIMAL, ConverterForBigDecimal.INSTANCE);
        result.put(DOUBLE, ConverterForDouble.SIGNED);
        result.put(FLOAT, ConverterForFloat.SIGNED);
        result.put(INT, LongConverter.INT);
        result.put(LONG, LongConverter.LONG);
        result.put(VARCHAR, ConverterForString.STRING);
        result.put(TEXT, ConverterForString.TEXT);
        result.put(TIME, LongConverter.TIME);
        result.put(TIMESTAMP, LongConverter.TIMESTAMP);
        result.put(INTERVAL_MILLIS, LongConverter.INTERVAL_MILLIS);
        result.put(INTERVAL_MONTH,LongConverter.INTERVAL_MONTH);
        result.put(U_BIGINT, ConverterForBigInteger.INSTANCE);
        result.put(U_DOUBLE, ConverterForDouble.UNSIGNED);
        result.put(U_FLOAT, ConverterForFloat.UNSIGNED);
        result.put(U_INT, LongConverter.U_INT);
        result.put(VARBINARY, ConverterForVarBinary.INSTANCE);
        result.put(YEAR, LongConverter.YEAR);
        result.put(BOOL, new ConverterForBool());
        result.put(RESULT_SET, ConverterForResultSet.INSTANCE);
        return result;
    }

    private static Map<AkType,Set<AkType>> createLegalConversionsMap() {
        
        ConversionsBuilder builder = new ConversionsBuilder(NULL, UNSUPPORTED);
        
        builder.alias(VARCHAR, TEXT);
        builder.alias(DOUBLE, U_DOUBLE);
        builder.alias(FLOAT, U_FLOAT);
        builder.alias(LONG, INT);
        builder.alias(LONG, U_INT);
        
        builder.legalConversions(VARBINARY,
                VARCHAR);
        builder.legalConversions(VARCHAR,
                VARCHAR,
                BOOL,
                DOUBLE,
                FLOAT,
                LONG,
                U_BIGINT,
                TIME,
                TIMESTAMP,
                INTERVAL_MILLIS,
                INTERVAL_MONTH,
                YEAR,
                DATE,
                DATETIME,
                DECIMAL,
                VARBINARY
        );
        builder.legalConversions(U_BIGINT,
                DATE,
                TIME,
                DATETIME,
                TIMESTAMP,
                YEAR,
                FLOAT,
                DOUBLE,
                DECIMAL,
                DOUBLE,
                VARCHAR,
                LONG,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(DECIMAL,
                DATE,
                TIME,
                DATETIME,
                TIMESTAMP,
                YEAR,
                U_BIGINT,
                VARCHAR,
                LONG,
                INTERVAL_MILLIS,
                INTERVAL_MONTH,
                FLOAT,
                DOUBLE
        );
        builder.legalConversions(DOUBLE,
                DATE,
                TIME,
                DATETIME,
                TIMESTAMP,
                YEAR,
                FLOAT,
                DECIMAL,
                LONG,
                U_BIGINT,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(FLOAT,
                DATE,
                TIME,
                DATETIME,
                TIMESTAMP,
                YEAR,
                DOUBLE,
                DECIMAL,
                LONG,
                U_BIGINT,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(LONG,
                DATE,
                TIME,
                DATETIME,
                TIMESTAMP,
                YEAR,
                DOUBLE,
                FLOAT,
                U_BIGINT,
                DECIMAL,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(DATE,
                DOUBLE,
                DECIMAL,
                FLOAT,
                DATETIME,
                TIMESTAMP,
                VARCHAR,
                LONG
        );
        builder.legalConversions(DATETIME,
                DOUBLE,
                DECIMAL,
                FLOAT,
                DATE,
                TIMESTAMP,
                VARCHAR,
                LONG
        );
        builder.legalConversions(TIME,
                DOUBLE,
                DECIMAL,
                FLOAT,
                DATE,
                YEAR,
                DATETIME,
                TIMESTAMP,
                VARCHAR,
                LONG
        );
        builder.legalConversions(TIMESTAMP,
                DOUBLE,
                DECIMAL,
                FLOAT,
                DATETIME,
                DATE,
                VARCHAR,
                LONG
        );
        builder.legalConversions(YEAR,
                DOUBLE,
                DECIMAL,
                FLOAT,
                DATE,
                DATETIME,
                TIMESTAMP,
                VARCHAR,
                LONG
        );
        builder.legalConversions(BOOL,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH,
                LONG,
                DOUBLE,
                FLOAT,
                DECIMAL,
                U_BIGINT,
                DATE,
                TIME,
                DATETIME,
                TIMESTAMP,
                YEAR
        );

        builder.legalConversions(INTERVAL_MILLIS,
                FLOAT,
                DOUBLE,
                DECIMAL,
                U_BIGINT,                
                LONG,
                VARCHAR               
        );

        builder.legalConversions(INTERVAL_MONTH,
                FLOAT,
                DOUBLE,
                DECIMAL,
                U_BIGINT,
                LONG,
                VARCHAR
        );
        
        return builder.result();
    }
    
    private Converters() {}
    
    // class state
    public static final String DEFAULT_CS = "latin1";
    
    /**
     * A mapping of AkTypes to converters. This map must never be modified once it's created -- the instance's
     * thread safety comes solely from the happens-before relationship of the class instantiation.
     */
    private static final Map<AkType,AbstractConverter> readOnlyConvertersMap = createConvertersMap();

    /**
     * Mapping of each source AkType to the set of AkTypes it can be converted to. Unsynchronized, so read only.
     */
    private static final Map<AkType,Set<AkType>> readOnlyLegalConversions = createLegalConversionsMap();
}
