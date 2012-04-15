/*
 * Copyright (c) 2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package krati.store.avro.protocol;

import org.apache.avro.util.Utf8;

/**
 * ProtocolConstants
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public final class ProtocolConstants {
    public final static String TYPE_KeyValue      = "KeyValue";
    public final static String TYPE_KeyValueClock = "KeyValueClock";
    public final static String TYPE_SyncResultSet = "SyncResultSet";
    
    public final static String MSG_GET  = "get";
    public final static String MSG_PUT  = "put";
    public final static String MSG_DEL  = "del";
    public final static String MSG_MGET = "mget";
    public final static String MSG_MPUT = "mput";
    public final static String MSG_MDEL = "mdel";
    public final static String MSG_SYNC = "sync";
    public final static String MSG_META = "meta";
    
    public final static String NOP = "NOP";
    public final static String SUC = "SUC";
    public final static String ERR = "ERR";
    
    public final static Utf8 NOP_UTF8 = new Utf8(ProtocolConstants.NOP);
    public final static Utf8 SUC_UTF8 = new Utf8(ProtocolConstants.SUC);
    public final static Utf8 ERR_UTF8 = new Utf8(ProtocolConstants.ERR);
    
    public final static String OPT_GET_PROPERTY = "-XP:";
    public final static String OPT_SET_PROPERTY = "-XP:+";
    public final static String OPT_DEL_PROPERTY = "-XP:-";
    
    public final static Utf8 OPT_GET_PROPERTY_UTF8 = new Utf8(OPT_GET_PROPERTY);
    public final static Utf8 OPT_SET_PROPERTY_UTF8 = new Utf8(OPT_SET_PROPERTY);
    public final static Utf8 OPT_DEL_PROPERTY_UTF8 = new Utf8(OPT_DEL_PROPERTY);
}
