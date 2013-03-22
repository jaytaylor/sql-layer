
package com.akiban.server.types.extract;

public final class ConverterTestUtils {
    public static void setGlobalTimezone(String timezone) {
        ExtractorsForDates.setGlobalTimezone(timezone);
    }

    private ConverterTestUtils() {}
}
