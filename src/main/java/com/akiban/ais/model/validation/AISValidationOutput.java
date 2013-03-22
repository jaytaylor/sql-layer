
package com.akiban.ais.model.validation;


public interface AISValidationOutput {
    public void reportFailure(AISValidationFailure failure);
}