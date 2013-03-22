
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;

public interface AISValidation {
    /**
     * Validates the given AIS.
     * @param ais   the AkibanInformationSchema to validate
     * @param output the AISValidationFailure collection holder, where
     *          any validations failures are registered. 
     */
    void validate(AkibanInformationSchema ais, AISValidationOutput output);
}
