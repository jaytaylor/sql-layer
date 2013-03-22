
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
