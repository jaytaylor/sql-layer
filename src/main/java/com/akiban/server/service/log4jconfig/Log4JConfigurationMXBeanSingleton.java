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

package com.akiban.server.service.log4jconfig;

public class Log4JConfigurationMXBeanSingleton implements Log4JConfigurationMXBean {
    private final static Log4JConfigurationMXBean instance = new Log4JConfigurationMXBeanSingleton();

    public static Log4JConfigurationMXBean instance() {
        return instance;
    }

    Log4JConfigurationMXBeanSingleton() {
        // nothing
    }

    private final Object MONITOR = new Object();
    private Long updateFrequency = null;
    private String configFile = null;

    @Override
    public final String getConfigurationFile() {
        synchronized (MONITOR) {
            return configFile;
        }
    }

    @Override
    public final void setConfigurationFile(String configFile) {
        if (configFile == null) {
            throw new IllegalArgumentException("config file may not be null");
        }
        synchronized (MONITOR) {
            if(this.updateFrequency != null) {
                throw new IllegalStateException("Can't set config file after polling has started");
            }
            this.configFile = configFile;
        }
        configure(configFile);
    }

    @Override
    public final Long getUpdateFrequencyMS() {
        synchronized (MONITOR) {
            return updateFrequency;
        }
    }

    @Override
    public final void pollConfigurationFile(String file, long updateFrequencyMS) {
        if (file == null) {
            throw new IllegalArgumentException("file may not be null");
        }
        if (updateFrequencyMS <= 0) {
            throw new IllegalArgumentException("updateFrequencyMS must be positive (tried to pass in "
                    + updateFrequencyMS + ')');
        }

        synchronized (MONITOR) {
            if (this.updateFrequency != null) {
                throw new IllegalStateException("Can't set config file or polling frequency after polling has started");
            }
            this.configFile = file;
            this.updateFrequency = updateFrequencyMS;
        }

        configureAndWatch(file, updateFrequencyMS);
    }

    @Override
    public final void pollConfigurationFile(long updateFrequencyMS) {
        final String configFileLocal = getConfigurationFile();
        if (configFileLocal == null) {
            throw new IllegalStateException("can't start polling until you set a config file");
        }
        pollConfigurationFile(configFileLocal, updateFrequencyMS);
    }

    @Override
    public final void updateConfigurationFile() {
        String configFileLocal = getConfigurationFile();
        if (configFileLocal == null) {
            throw new IllegalStateException("can't update file until it's set");
        }
        configure(configFileLocal);
    }

    protected void configure(String configFile) {
        new Throwable(configFile).printStackTrace();
        org.apache.log4j.PropertyConfigurator.configure(configFile);
    }

    protected void configureAndWatch(String configFile, long updateFrequency) {
        org.apache.log4j.PropertyConfigurator.configureAndWatch(configFile, updateFrequency);
    }
}
