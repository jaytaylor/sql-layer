
package com.akiban.server.service.log4jconfig;

public interface Log4JConfigurationMXBean {
    String getConfigurationFile();
    void setConfigurationFile(String configFile);

    Long getUpdateFrequencyMS();

    void pollConfigurationFile(String file, long updateFrequencyMS);
    void pollConfigurationFile(long updateFrequencyMS);

    void updateConfigurationFile();
}
