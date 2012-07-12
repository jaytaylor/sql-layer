/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

public interface TClassFormatter {
    
    public void format(TInstance instance, PValueSource source, AkibanAppender out);
}
