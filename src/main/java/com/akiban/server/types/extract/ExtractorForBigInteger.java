
package com.akiban.server.types.extract;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;

final class ExtractorForBigInteger extends ObjectExtractor<BigInteger> {
    @Override
    public BigInteger getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case U_BIGINT:  return source.getUBigInt();
        case LONG:      return BigInteger.valueOf(source.getLong());
        case INT:       return BigInteger.valueOf(source.getInt());
        case U_INT:     return BigInteger.valueOf(source.getUInt());
        case TEXT:      return new BigInteger(source.getText());
        case VARCHAR:   return new BigInteger(source.getString());
        case INTERVAL_MILLIS:  return BigInteger.valueOf(source.getInterval_Millis());
        case INTERVAL_MONTH:   return BigInteger.valueOf(source.getInterval_Month());
        case DECIMAL:   return source.getDecimal().setScale(0, BigDecimal.ROUND_HALF_UP).toBigInteger();
        case U_DOUBLE:  return BigInteger.valueOf(Math.round(source.getUDouble()));
        case DOUBLE:    return BigInteger.valueOf(Math.round(source.getDouble()));
        case FLOAT:     return BigInteger.valueOf(Math.round(source.getFloat()));
        case U_FLOAT:   return BigInteger.valueOf(Math.round(source.getUFloat()));
        case DATE:      return BigInteger.valueOf(source.getDate());
        case DATETIME:  return BigInteger.valueOf(source.getDateTime());
        case TIME:      return BigInteger.valueOf(source.getTime());
        case TIMESTAMP: return BigInteger.valueOf(source.getTimestamp());
        case YEAR:      return BigInteger.valueOf(source.getYear());
        default: throw unsupportedConversion(type);
        }
    }

    @Override
    public BigInteger getObject(String string) {
        return new BigInteger(string);
    }

    ExtractorForBigInteger() {
        super(AkType.U_BIGINT);
    }
}
