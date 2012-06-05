/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.types3.TAttributeValue;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TypeDeclarationException;
import java.util.List;
import java.util.Locale;

public class StringFactory implements TFactory
{
    //--------------------------------CHARSET-----------------------------------
    //TODO: add more charsets as needed
    private static final String CHARSETS[] = {"UTF-8", "UTF-16", "LATIN1"};
    
    // Available CHARSET IDs
    // If something is added here, the corresponding charset name has to be added
    // to CHARSETS[], too
    public static final int UTF8 = 0;
    
    public static final int UTF16 = 1;
    
    public static final int LATIN1 = 2;
    
    //--------------------------------LOCALE------------------------------------
    private static final Locale LOCALES[] = Locale.getAvailableLocales();
    
    // Locale IDs (generated)
    public static final int ja_JP = 0;
    public static final int es_PE = 1;
    public static final int en = 2;
    public static final int ja_JP_JP = 3;
    public static final int es_PA = 4;
    public static final int sr_BA = 5;
    public static final int mk = 6;
    public static final int es_GT = 7;
    public static final int ar_AE = 8;
    public static final int no_NO = 9;
    public static final int sq_AL = 10;
    public static final int bg = 11;
    public static final int ar_IQ = 12;
    public static final int ar_YE = 13;
    public static final int hu = 14;
    public static final int pt_PT = 15;
    public static final int el_CY = 16;
    public static final int ar_QA = 17;
    public static final int mk_MK = 18;
    public static final int sv = 19;
    public static final int de_CH = 20;
    public static final int en_US = 21;
    public static final int fi_FI = 22;
    public static final int is = 23;
    public static final int cs = 24;
    public static final int en_MT = 25;
    public static final int sl_SI = 26;
    public static final int sk_SK = 27;
    public static final int it = 28;
    public static final int tr_TR = 29;
    public static final int zh = 30;
    public static final int th = 31;
    public static final int ar_SA = 32;
    public static final int no = 33;
    public static final int en_GB = 34;
    public static final int sr_CS = 35;
    public static final int lt = 36;
    public static final int ro = 37;
    public static final int en_NZ = 38;
    public static final int no_NO_NY = 39;
    public static final int lt_LT = 40;
    public static final int es_NI = 41;
    public static final int nl = 42;
    public static final int ga_IE = 43;
    public static final int fr_BE = 44;
    public static final int es_ES = 45;
    public static final int ar_LB = 46;
    public static final int ko = 47;
    public static final int fr_CA = 48;
    public static final int et_EE = 49;
    public static final int ar_KW = 50;
    public static final int sr_RS = 51;
    public static final int es_US = 52;
    public static final int es_MX = 53;
    public static final int ar_SD = 54;
    public static final int in_ID = 55;
    public static final int ru = 56;
    public static final int lv = 57;
    public static final int es_UY = 58;
    public static final int lv_LV = 59;
    public static final int iw = 60;
    public static final int pt_BR = 61;
    public static final int ar_SY = 62;
    public static final int hr = 63;
    public static final int et = 64;
    public static final int es_DO = 65;
    public static final int fr_CH = 66;
    public static final int hi_IN = 67;
    public static final int es_VE = 68;
    public static final int ar_BH = 69;
    public static final int en_PH = 70;
    public static final int ar_TN = 71;
    public static final int fi = 72;
    public static final int de_AT = 73;
    public static final int es = 74;
    public static final int nl_NL = 75;
    public static final int es_EC = 76;
    public static final int zh_TW = 77;
    public static final int ar_JO = 78;
    public static final int be = 79;
    public static final int is_IS = 80;
    public static final int es_CO = 81;
    public static final int es_CR = 82;
    public static final int es_CL = 83;
    public static final int ar_EG = 84;
    public static final int en_ZA = 85;
    public static final int th_TH = 86;
    public static final int el_GR = 87;
    public static final int it_IT = 88;
    public static final int ca = 89;
    public static final int hu_HU = 90;
    public static final int fr = 91;
    public static final int en_IE = 92;
    public static final int uk_UA = 93;
    public static final int pl_PL = 94;
    public static final int fr_LU = 95;
    public static final int nl_BE = 96;
    public static final int en_IN = 97;
    public static final int ca_ES = 98;
    public static final int ar_MA = 99;
    public static final int es_BO = 100;
    public static final int en_AU = 101;
    public static final int sr = 102;
    public static final int zh_SG = 103;
    public static final int pt = 104;
    public static final int uk = 105;
    public static final int es_SV = 106;
    public static final int ru_RU = 107;
    public static final int ko_KR = 108;
    public static final int vi = 109;
    public static final int ar_DZ = 110;
    public static final int vi_VN = 111;
    public static final int sr_ME = 112;
    public static final int sq = 113;
    public static final int ar_LY = 114;
    public static final int ar = 115;
    public static final int zh_CN = 116;
    public static final int be_BY = 117;
    public static final int zh_HK = 118;
    public static final int ja = 119;
    public static final int iw_IL = 120;
    public static final int bg_BG = 121;
    public static final int in = 122;
    public static final int mt_MT = 123;
    public static final int es_PY = 124;
    public static final int sl = 125;
    public static final int fr_FR = 126;
    public static final int cs_CZ = 127;
    public static final int it_CH = 128;
    public static final int ro_RO = 129;
    public static final int es_PR = 130;
    public static final int en_CA = 131;
    public static final int de_DE = 132;
    public static final int ga = 133;
    public static final int de_LU = 134;
    public static final int de = 135;
    public static final int es_AR = 136;
    public static final int sk = 137;
    public static final int ms_MY = 138;
    public static final int hr_HR = 139;
    public static final int en_SG = 140;
    public static final int da = 141;
    public static final int mt = 142;
    public static final int pl = 143;
    public static final int ar_OM = 144;
    public static final int tr = 145;
    public static final int th_TH_TH = 146;
    public static final int el = 147;
    public static final int ms = 148;
    public static final int sv_SE = 149;
    public static final int da_DK = 150;
    public static final int es_HN = 151;
    
    //--------------------------------COLLATI-----------------------------------
    // TODO: not sure yet what we want to do about this
    
    //------------------------------Default values------------------------------
    
    // default number of characters in a string      
    private static final int DEFAULT_LENGTH = 256;
    
    private static final int DEFAULT_CHARSET_ID = UTF8;
    
    private static final int DEFAULT_LOCALE_ID = en_US;
    
    private static final int DEFAULT_COLLATION_ID = 0; // TODO:
    
    //--------------------------------------------------------------------------
    
    private final TClass tclass;
    
    StringFactory(TClass tClass)
    {
        tclass = tClass;
    }
     
    /**
     * 
     * @param arguments: containing the attributes of a String. Should be in the following order:
     *          [<LENGTH>[,<CHARSET_ID>[,<LOCALE>[,<COLLATION>]]]]
     * @param strict ?? What's this flag for?
     * @return a type instance with the given attribute
     */
    @Override
    public TInstance create(List<TAttributeValue> arguments, boolean strict)
    {
        int length = DEFAULT_LENGTH;
        int charsetId = DEFAULT_CHARSET_ID;
        int locale = DEFAULT_LOCALE_ID;
        int collation = DEFAULT_COLLATION_ID;
       
        switch (arguments.size())
        {
            case 4: // everything available
                collation = arguments.get(3).intValue(); // fall thru
            case 3: // available up to locale 
                locale = arguments.get(2).intValue(); // fall thru
            case 2: // avaialble up to charset
                charsetId = arguments.get(1).intValue(); // fall thru
            case 1: // avaialble up to length 
                length = arguments.get(0).intValue(); // fall thru
            case 0: // nothing available
                break;
            default:
                throw new TypeDeclarationException("too many arguments");
        }
        return new TInstance(tclass, length, charsetId, locale, collation);
    }

}