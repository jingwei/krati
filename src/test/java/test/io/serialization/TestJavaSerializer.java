package test.io.serialization;

import java.util.HashMap;

import krati.io.Serializer;
import krati.io.serializer.JavaSerializer;

/**
 * TestJavaSerializer
 * 
 * @author jwu
 * 07/18, 2011
 * 
 */
public class TestJavaSerializer extends AbstractTestSerializer<HashMap<String, Object>> {
    
    @Override
    protected HashMap<String, Object> createObject() {
        HashMap<String,Object> userData = new HashMap<String,Object>();
        HashMap<String,String> nameStruct = new HashMap<String,String>();
        nameStruct.put("first", "Joe");
        nameStruct.put("last", "Sixpack");
        userData.put("name", nameStruct);
        userData.put("gender", "MALE");
        userData.put("verified", Boolean.FALSE);
        userData.put("userImage", "Rm9vYmFyIQ==");
        return userData;
    }
    
    @Override
    protected Serializer<HashMap<String, Object>> createSerializer() {
        return new JavaSerializer<HashMap<String, Object>>();
    }
}
