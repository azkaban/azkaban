/*
 * Copyright 2018 LinkedIn Corp.
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

package trigger.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Created a Kafka producer sending event record with schema for testing purpose 
 */
public class KafkaProducerTest {
  private final static String TOPIC = "AzEvent_Topic4";
  private final KafkaProducer producer;

  public KafkaProducerTest(final String name, final String username) {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    // Configure the KafkaAvroSerializer.
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    // Schema Registry location.
    this.producer = new KafkaProducer<>(props);
    final byte[] schemaRecord = this.createAvroRecord(name, username);
    final ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, schemaRecord);
    try {
      System.out.println("+++++++++++++++" + record + "+++++++++++++++");
      this.producer.send(record);
    } catch (final Exception ex) {
      System.out.println(ex);
    }
    this.producer.flush();
    this.producer.close();
  }

  public byte[] createAvroRecord(final String name, final String username) {
    final String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"},{\"name\": \"username\", \"type\": \"string\"}]}";
    final Schema.Parser parser = new Schema.Parser();
    final Schema schema = parser.parse(userSchema);
    final GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", name);
    avroRecord.put("username", username);

    final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    final ByteArrayOutputStream os = new ByteArrayOutputStream();
    final Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
    try {
      writer.write(avroRecord, encoder);
      encoder.flush();
      os.close();
    } catch (final IOException e) {
      e.printStackTrace();
    }
    return os.toByteArray();
  }
}

