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

package com.foundationdb.server.service.log4jconfig;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public final class Log4JConfigurationMXBeanSingletonTest {

    private static String sayConfigure(String configFile) {
        return "configure " + configFile;
    }

    private static String sayConfigAndWatch(String configFile, long frequency) {
        return "watch " + frequency + ' ' + configFile;
    }

    private static class TestConfigurator extends Log4JConfigurationMXBeanSingleton {
        private final List<String> messages = new ArrayList<>();

        @Override
        protected void configure(String configFile) {
            messages.add(sayConfigure(configFile));
        }

        @Override
        protected void configureAndWatch(String configFile, long updateFrequency) {
            messages.add(sayConfigAndWatch(configFile, updateFrequency));
        }

        void assertAll(String expectedConfig, Long expectedUpdate, String... expectedMessages) {
            org.junit.Assert.assertEquals("messages", Arrays.asList(expectedMessages), messages);
            org.junit.Assert.assertEquals("config file", expectedConfig, getConfigurationFile());
            org.junit.Assert.assertEquals("update freq", expectedUpdate, getUpdateFrequencyMS());
        }

        Log4JConfigurationMXBean bean() {
            return this;
        }
    }

    @Test
    public void startsNull() {
        new TestConfigurator().assertAll(null, null);
    }

    @Test
    public void setConfigThenPollThenTryAgain() {
        final TestConfigurator test = new TestConfigurator();
        test.setConfigurationFile("alpha");
        test.assertAll("alpha", null, sayConfigure("alpha"));
        test.bean().pollConfigurationFile("beta", 4);
        test.assertAll("beta", 4L, sayConfigure("alpha"), sayConfigAndWatch("beta", 4));

        expectException(IllegalStateException.class, new Runnable() {
            @Override
            public void run() {
                test.setConfigurationFile("gamma");
            }
        });

        test.assertAll("beta", 4L, sayConfigure("alpha"), sayConfigAndWatch("beta", 4));
    }

    @Test
    public void pollWithoutConfiguring() {
        final TestConfigurator test = new TestConfigurator();
        expectException(IllegalStateException.class, new Runnable() {
            @Override
            public void run() {
                test.bean().pollConfigurationFile(4);
            }
        });
        test.assertAll(null, null);
    }

    @Test
    public void pollNegative() {
        final TestConfigurator test = new TestConfigurator();
        test.setConfigurationFile("alpha");
        expectException(IllegalArgumentException.class, new Runnable() {
            @Override
            public void run() {
                test.bean().pollConfigurationFile(0);
            }
        });
        test.assertAll("alpha", null, sayConfigure("alpha"));
    }

    @Test
    public void nullConfig() {
        final TestConfigurator test = new TestConfigurator();

        expectException(IllegalArgumentException.class, new Runnable() {
            @Override
            public void run() {
                test.bean().setConfigurationFile(null);
            }
        });
        test.assertAll(null, null);
    }

    @Test
    public void nullConfigWithPoll() {
        final TestConfigurator test = new TestConfigurator();

        expectException(IllegalArgumentException.class, new Runnable() {
            @Override
            public void run() {
                test.bean().pollConfigurationFile(null, 4);
            }
        });
        test.assertAll(null, null);
    }

    @Test
    public void configurePollUpdate() {
        final TestConfigurator test = new TestConfigurator();
        final Log4JConfigurationMXBean bean = test.bean();

        bean.setConfigurationFile("alpha");
        test.assertAll("alpha", null, sayConfigure("alpha"));

        bean.pollConfigurationFile("beta", 5);
        test.assertAll("beta", 5L, sayConfigure("alpha"), sayConfigAndWatch("beta", 5));

        bean.updateConfigurationFile();
        test.assertAll("beta", 5L, sayConfigure("alpha"), sayConfigAndWatch("beta", 5), sayConfigure("beta"));
    }

    private static <E extends RuntimeException> void expectException(Class<E> exceptionClass, Runnable runnable) {
        Exception exception = null;
        try {
            runnable.run();
        } catch (Exception e) {
            exception = e;
        }
        assertNotNull("expected exception " + exceptionClass.getSimpleName(), exception);
        assertSame("expected exception class", exceptionClass, exception.getClass());
    }
}
