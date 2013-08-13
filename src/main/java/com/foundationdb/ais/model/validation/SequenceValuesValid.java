/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Sequence;
import com.akiban.server.error.SequenceIntervalZeroException;
import com.akiban.server.error.SequenceMinGEMaxException;
import com.akiban.server.error.SequenceStartInRangeException;

public class SequenceValuesValid implements AISValidation {

    @Override
    public void validate(AkibanInformationSchema ais, AISValidationOutput output) {
        for (Sequence sequence : ais.getSequences().values()) {
            if (sequence.getIncrement() == 0) {
                output.reportFailure(new AISValidationFailure (
                        new SequenceIntervalZeroException()));
            }
            if (sequence.getMinValue() >= sequence.getMaxValue()) {
                output.reportFailure(new AISValidationFailure (
                        new SequenceMinGEMaxException()));
            }
            if (sequence.getStartsWith() < sequence.getMinValue() ||
                    sequence.getStartsWith() > sequence.getMaxValue()) {
                output.reportFailure(new AISValidationFailure(
                        new SequenceStartInRangeException ()));
            }
                
                
        }
    }
}
