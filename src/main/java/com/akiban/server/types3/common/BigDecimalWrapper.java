/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.akiban.server.types3.common;

public interface BigDecimalWrapper {
    
     BigDecimalWrapper add(BigDecimalWrapper augend);
     BigDecimalWrapper subtract(BigDecimalWrapper augend);
     BigDecimalWrapper multiply(BigDecimalWrapper augend);
     BigDecimalWrapper divide(BigDecimalWrapper augend);
     void reset();
}
