
package com.akiban.server.types.extract;

import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.error.OverflowException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;

public final class DoubleExtractor extends AbstractExtractor {

    public double getDouble(ValueSource source) {
        if (source.isNull())
            throw new ValueSourceIsNullException();

        AkType type = source.getConversionType();
        switch (type) {
        case DECIMAL:   return getDecimalAsDouble(source);
        case DOUBLE:    return source.getDouble();
        case FLOAT:     return source.getFloat();
        case INT:       return source.getInt();
        case LONG:      return source.getLong();
        case VARCHAR:   return getDouble(source.getString());
        case TEXT:      return getDouble(source.getText());
        case U_BIGINT:  return getBigIntAsDouble(source);
        case U_DOUBLE:  return source.getUDouble();
        case U_FLOAT:   return source.getUFloat();
        case U_INT:     return source.getUInt();
        case INTERVAL_MILLIS:  return source.getInterval_Millis();
        case INTERVAL_MONTH:   return source.getInterval_Month();
        case DATE:      return source.getDate();
        case DATETIME:  return source.getDateTime();
        case TIME:      return source.getTime();
        case TIMESTAMP: return source.getTimestamp();
        case YEAR:      return source.getYear();
        default:
            throw unsupportedConversion(type);
        }                
    }

    private double getDecimalAsDouble (ValueSource source )
    {
        double d = source.getDecimal().doubleValue();
        if (Double.isInfinite(d))
            throw new OverflowException();        
        else
            return d;
    }

    private double getBigIntAsDouble (ValueSource source)
    {
        double d = source.getUBigInt().doubleValue();
        if (Double.isInfinite(d))
            throw new OverflowException();        
        else
            return d;
    }

    public double getDouble(String string) {
        try
        {
            return Double.parseDouble(string);
        }
        catch (NumberFormatException e)
        {
            throw new InvalidCharToNumException(e.getMessage());
        }
    }

    public String asString(double value) {
        return Double.toString(value);
    }

    // package-private ctor
    DoubleExtractor() {
        super(AkType.DOUBLE);
    }
}
