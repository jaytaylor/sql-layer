package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Hex;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class MHex extends TScalarBase {
  public static final TScalar[] INSTANCES = Hex.create(MString.VARCHAR, MNumeric.BIGINT);
}
