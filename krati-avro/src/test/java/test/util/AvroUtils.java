package test.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.avro.Schema;

/**
 * AvroUtils
 * 
 * @author jwu
 * @since 09/27, 2012
 */
public class AvroUtils {

    public static Schema loadSchema(File schemaFile) throws IOException {
        FileInputStream fis = new FileInputStream(schemaFile);
        return Schema.parse(fis);
    }
}
