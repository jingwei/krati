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

package krati.store.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import krati.io.SerializationException;
import krati.io.Serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;

/**
 * AvroGenericRecordSerializer performs the serialization and de-serialization of an Avro
 * {@link GenericRecord} using the {@link org.apache.avro.io.BinaryDecoder BinaryDecoder}
 * and {@link org.apache.avro.io.BinaryEncoder BinaryEncoder} respectively.
 * 
 * @author jwu
 * @since 08/06, 2011
 */
public class AvroGenericRecordSerializer implements Serializer<GenericRecord> {
    private final Schema _schema;
    
    /**
     * Creates a new instance of AvroGenericRecordSerializer.
     * 
     * @param schema - the Avro {@link Schema}
     */
    public AvroGenericRecordSerializer(Schema schema) {
        this._schema = schema;
    }
    
    /**
     * Gets the Avro {@link Schema} known to this serializer.
     */
    public final Schema getSchema() {
        return _schema;
    }
    
    @Override
    public GenericRecord deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null) {
            return null;
        }
        
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(in, null);
            GenericDatumReader<Record> datumReader = new GenericDatumReader<Record>(_schema);
            GenericData.Record record = new GenericData.Record(_schema);
            
            datumReader.read(record, decoder);
            
            return record;
        } catch(Exception e) {
            throw new SerializationException("Failed to deserialize", e);
        }
    }

    @Override
    public byte[] serialize(GenericRecord record) throws SerializationException {
        if(record == null) {
            return null;
        }
        
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = new BinaryEncoder(out); 
            GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(_schema);
            
            datumWriter.write(record, encoder);
            encoder.flush();
            
            return out.toByteArray();
        } catch(Exception e) {
            throw new SerializationException("Failed to serialize", e);
        }
    }
}
