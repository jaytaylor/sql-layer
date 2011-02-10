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

package com.akiban.cserver.service.tree;

import com.persistit.logging.AbstractPersistitLogger;
import com.persistit.logging.LogTemplate;
import org.slf4j.Logger;

public final class PersistitSlf4jAdapter extends AbstractPersistitLogger {
    private final Logger logger;

    public PersistitSlf4jAdapter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void log(LogTemplate logTemplate, String message) {
        switch(logTemplate.getLevel()) {
            case FINEST:
            case FINER:
                logger.trace(message);
                break;
            case FINE:
                logger.debug(message);
                break;
            case INFO:
                logger.info(message);
                break;
            case WARNING:
                logger.warn(message);
                break;
            case SEVERE:
                logger.error(message);
            case ALWAYS:
                logger.error("Persistit ALWAYS-level message: {}", message);
                break;
            default:
                logger.warn("Unknown level {} with message: {}", logTemplate.getLevel(), message);
                break;
        }
    }
}
