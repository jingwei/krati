/*
 * Copyright (c) 2010-2012 LinkedIn, Inc
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

package test.store.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.node.TextNode;

import junit.framework.TestCase;
import krati.io.SerializationException;
import krati.io.Serializer;
import krati.store.avro.AvroGenericRecordResolvingSerializer;
import krati.store.avro.AvroGenericRecordSerializer;

/**
 * TestGenericRecordSerializer
 * 
 * @author jwu
 * @since 02/01, 2012
 */
public class TestGenericRecordSerializer extends TestCase {
    static Random _rand = new Random();
    
    static Schema createSchemaV1() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("id", Schema.create(Type.INT), null, null));
        fields.add(new Field("age", Schema.create(Type.INT), null, null));
        fields.add(new Field("fname", Schema.create(Type.STRING), null, null));
        fields.add(new Field("lname", Schema.create(Type.STRING), null, null));
        
        Schema schema = Schema.createRecord("Person", null, "avro.test", false);
        schema.setFields(fields);
        
        return schema;
    }
    
    static Schema createSchemaV2() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("id", Schema.create(Type.INT), null, null));
        fields.add(new Field("age", Schema.create(Type.INT), null, null));
        fields.add(new Field("fname", Schema.create(Type.STRING), null, null));
        fields.add(new Field("lname", Schema.create(Type.STRING), null, null));
        fields.add(new Field("title", Schema.create(Type.STRING), null, new TextNode("")));
        
        Schema schema = Schema.createRecord("Person", null, "avro.test", false);
        schema.setFields(fields);
        
        return schema;
    }
    
    static GenericRecord createRecordV1(Schema schema, int memberId) {
        GenericData.Record record = new GenericData.Record(schema);
        
        record.put("id", memberId);
        record.put("age", _rand.nextInt(100));
        record.put("fname", new Utf8("firstName." + memberId));
        record.put("lname", new Utf8("lastName." + memberId));
        
        return record;
    }
    
    static GenericRecord createRecordV2(Schema schema, int memberId) {
        GenericData.Record record = new GenericData.Record(schema);
        
        record.put("id", memberId);
        record.put("age", _rand.nextInt(100));
        record.put("fname", new Utf8("firstName." + memberId));
        record.put("lname", new Utf8("lastName." + memberId));
        record.put("title", new Utf8("title." + memberId));
        
        return record;
    }
    
    /**
     * Tests the behavior of {@link AvroGenericRecordResolvingSerializer} on
     * Schema evolution.
     */
    public void testGenericRecordResolvingSerializer() throws IOException {
        byte[] writerBytes;
        
        Schema writerSchema = createSchemaV1();
        Schema readerSchema = createSchemaV2();
        
        Serializer<GenericRecord> writerSerializer =
            new AvroGenericRecordSerializer(writerSchema);
        
        Serializer<GenericRecord> readerSerializer =
            new AvroGenericRecordSerializer(readerSchema);
        
        Serializer<GenericRecord> resolvingSerializer =
            new AvroGenericRecordResolvingSerializer(writerSchema, readerSchema);
        
        GenericRecord record = createRecordV1(writerSchema, 1);
        writerBytes = writerSerializer.serialize(record);
        
        GenericRecord record1 = writerSerializer.deserialize(writerBytes);
        assertEquals(record.get("id"), record1.get("id"));
        assertEquals(record.get("age"), record1.get("age"));
        assertEquals(record.get("fname"), record1.get("fname"));
        assertEquals(record.get("lname"), record1.get("lname"));
        
        GenericRecord record2 = null;
        try {
            record2 = readerSerializer.deserialize(writerBytes);
        } catch(SerializationException e) {}
        assertNull(record2);
        
        GenericRecord record3 = resolvingSerializer.deserialize(writerBytes);
        assertEquals(record.get("id"), record3.get("id"));
        assertEquals(record.get("age"), record3.get("age"));
        assertEquals(record.get("fname"), record3.get("fname"));
        assertEquals(record.get("lname"), record3.get("lname"));
    }
    
    /**
     * Tests forward compatibility.
     * <p>
     * A legacy {@link GenericRecord} reader (based on a legacy schema) can deserialize bytes
     * produced by a {@link GenericRecord} serializer with the same legacy schema or a newer
     * serializer based on an updated schema that adds new fields with default values to the
     * legacy schema.
     * </p> 
     */
    public void testForwardCompatibility() {
        byte[] writerBytes;
        
        Schema schemaV1 = createSchemaV1();
        Schema schemaV2 = createSchemaV2();
        
        Serializer<GenericRecord> writerSerializer =
            new AvroGenericRecordSerializer(schemaV1);
        
        GenericRecord record = createRecordV1(schemaV1, 1);
        writerBytes = writerSerializer.serialize(record);
        
        Serializer<GenericRecord> readerSerializer =
            new AvroGenericRecordSerializer(schemaV1);
        GenericRecord record1 = readerSerializer.deserialize(writerBytes);
        assertEquals(record.get("id"), record1.get("id"));
        assertEquals(record.get("age"), record1.get("age"));
        assertEquals(record.get("fname"), record1.get("fname"));
        assertEquals(record.get("lname"), record1.get("lname"));
        
        record = createRecordV2(schemaV2, 1);
        writerBytes = writerSerializer.serialize(record);
        
        GenericRecord record2 = readerSerializer.deserialize(writerBytes);
        assertEquals(record.get("id"), record2.get("id"));
        assertEquals(record.get("age"), record2.get("age"));
        assertEquals(record.get("fname"), record2.get("fname"));
        assertEquals(record.get("lname"), record2.get("lname"));
    }
}
