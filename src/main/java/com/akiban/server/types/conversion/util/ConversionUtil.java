
package com.akiban.server.types.conversion.util;

import org.joda.time.MutableDateTime;

public class ConversionUtil 
{
    private static final AbstractConverter<MutableDateTime> DATETIME = new DateTimeConverter();
    
    public static AbstractConverter<MutableDateTime> getDateTimeConverter ()
    {
        return DATETIME;
    }
}
