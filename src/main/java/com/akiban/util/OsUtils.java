
package com.akiban.util;

import java.lang.management.ManagementFactory;

public class OsUtils {
    
    /**
     * @return the process ID of the current JVM
     */
    public static String getProcessID() {
        String pidHost = ManagementFactory.getRuntimeMXBean().getName();
        /*
         * string return from getName will always be of format
         * pid@hostname
         * tested to work on both Sun 6 and OpenJDK 6
         */
        int pidLocation = 0;
        return pidHost.split("@")[pidLocation];
    }

}
