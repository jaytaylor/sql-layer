/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.gwtutils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Static-method class for getting GWT-compatible loggers.</p>
 *
 * <p>At some point before you get a logger, you must register a {@link GwtLogFactory} object with this class.
 * {@link #getLogger(Class)} will then return a logger that's comprised of all registered factories' loggers (if
 * there's only one registered factory, {@linkplain #getLogger(Class)} is allowed to simply return that factory's
 * logger).</p>
 */
public class GwtLogging
{
    /**
     * Enums to represent our various levels. Use however you wish.
     */
    public enum LogLevel implements Serializable
    {
        TRACE, DEBUG, INFO, WARN, ERROR, FATAL
    }
    private final static List<GwtLogFactory> installed = new ArrayList<GwtLogFactory>();

    public static GwtLogger getLogger(Class<?> clazz) {
        return installed.size() == 1
               ? installed.get(0).getLogger(clazz)
               : new DelegatingLogger(clazz, installed);
    }

    public static void installFactory(GwtLogFactory factory) {
        installed.add(factory);
    }

    /**
     * A logger that delegates to any number of other loggers. Any exceptions caused by those loggers will be
     * silently swallowed up and ignored. The <tt>is*Enabled()</tt> methods will each iterate through all loggers
     * and return whether any of them are enabled for that level. This iteration will short-circuit at the first
     * logger that's enabled for the level, so your logger should not assume its <tt>is*Enabled()</tt>
     * method will be invoked.
     */
    public static final class DelegatingLogger implements GwtLogger
    {
        private final List<GwtLogger> loggers;

        public DelegatingLogger(Class<?> clazz, List<GwtLogFactory> factories) {
            loggers = new ArrayList<GwtLogger>(factories.size());
            for (GwtLogFactory factory : factories) {
                loggers.add( factory.getLogger(clazz) );
            }
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(DelegatingLogger.class.getName()).append('[');

            for (GwtLogger logger : loggers) {
                builder.append(logger).append("( ");
                if (logger.isTraceEnabled())    { builder.append("TRACE"); }
                if (logger.isDebugEnabled())    { builder.append("DEBUG"); }
                if (logger.isInfoEnabled())     { builder.append("INFO"); }
                if (logger.isWarnEnabled())     { builder.append("WARN"); }
                if (logger.isErrorEnabled())    { builder.append("ERROR"); }
                if (logger.isFatalEnabled())    { builder.append("FATAL"); }
                builder.append(" )");
            }

            builder.append(']');
            return builder.toString();
        }

        @Override
        public boolean isTraceEnabled() {
            for (GwtLogger logger : loggers) {
                try{ if (logger.isTraceEnabled()) return true; } catch (Throwable err) { /* swallow */ }
            }
            return false;
        }

        @Override
        public boolean isDebugEnabled() {
            for (GwtLogger logger : loggers) {
                try{ if (logger.isDebugEnabled()) return true; } catch (Throwable err) { /* swallow */ }
            }
            return false;
        }

        @Override
        public boolean isInfoEnabled() {
            for (GwtLogger logger : loggers) {
                try{ if (logger.isInfoEnabled()) return true; } catch (Throwable err) { /* swallow */ }
            }
            return false;
        }

        @Override
        public boolean isWarnEnabled() {
            for (GwtLogger logger : loggers) {
                try{ if (logger.isWarnEnabled()) return true; } catch (Throwable err) { /* swallow */ }
            }
            return false;
        }

        @Override
        public boolean isErrorEnabled() {
            for (GwtLogger logger : loggers) {
                try{ if (logger.isErrorEnabled()) return true; } catch (Throwable err) { /* swallow */ }
            }
            return false;
        }

        @Override
        public boolean isFatalEnabled() {
            for (GwtLogger logger : loggers) {
                try{ if (logger.isFatalEnabled()) return true; } catch (Throwable err) { /* swallow */ }
            }
            return false;
        }

        @Override
        public void trace(Object message) {
            for (GwtLogger logger : loggers) {
                try{ logger.trace(message); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void trace(Object message, Throwable t) {
            for (GwtLogger logger : loggers) {
                try{ logger.trace(message, t); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void debug(Object message) {
            for (GwtLogger logger : loggers) {
                try{ logger.debug(message); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void debug(Object message, Throwable t) {
            for (GwtLogger logger : loggers) {
                try{ logger.debug(message, t); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void info(Object message) {
            for (GwtLogger logger : loggers) {
                try{ logger.info(message); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void info(Object message, Throwable t) {
            for (GwtLogger logger : loggers) {
                try{ logger.info(message, t); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void warn(Object message) {
            for (GwtLogger logger : loggers) {
                try{ logger.warn(message); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void warn(Object message, Throwable t) {
            for (GwtLogger logger : loggers) {
                try{ logger.warn(message, t); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void error(Object message) {
            for (GwtLogger logger : loggers) {
                try{ logger.error(message); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void error(Object message, Throwable t) {
            for (GwtLogger logger : loggers) {
                try{ logger.error(message, t); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void fatal(Object message) {
            for (GwtLogger logger : loggers) {
                try{ logger.fatal(message); } catch (Throwable err) { /* swallow */ }
            }
        }

        @Override
        public void fatal(Object message, Throwable t) {
            for (GwtLogger logger : loggers) {
                try{ logger.fatal(message, t); } catch (Throwable err) { /* swallow */ }
            }
        }
    }
}
