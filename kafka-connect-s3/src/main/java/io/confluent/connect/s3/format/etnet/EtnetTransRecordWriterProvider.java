/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.format.etnet;

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;
import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.S3RetriableRecordWriter;
import io.confluent.connect.s3.storage.S3ParquetOutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EtnetTransRecordWriterProvider extends RecordViewSetter
    implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(EtnetTransRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private static final int PAGE_SIZE = 64 * 1024;
  private final S3Storage storage;
  static final String avroSchemaString = "{\"type\":\"record\"," 
      + "\"name\":\"Message\"," 
      + "\"fields\":["
      + "{\"name\":\"timestamp\",\"type\":\"long\"}," 
      + "{\"name\":\"key\",\"type\":\"string\"},"
      + "{\"name\":\"value\",\"type\":\"string\"}" 
      + "]}";
  private final org.apache.avro.Schema avroSchema = 
      new org.apache.avro.Schema.Parser().parse(avroSchemaString);

  EtnetTransRecordWriterProvider(S3Storage storage) {
    this.storage = storage;
  }

  @Override
  public String getExtension() {
    return storage.conf().parquetCompressionCodecName().getExtension() + EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    return new S3RetriableRecordWriter(
        new IORecordWriter() {
          final String adjustedFilename = getAdjustedFilename(recordView, filename, getExtension());
          ParquetWriter<GenericRecord> writer;
          S3ParquetOutputFile s3ParquetOutputFile;

          @Override
          public void write(SinkRecord record) throws IOException {
            if (writer == null) {
              log.info("Opening record writer for: {}", adjustedFilename);
              s3ParquetOutputFile = new S3ParquetOutputFile(storage, adjustedFilename);
              AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter
                        .<GenericRecord>builder(s3ParquetOutputFile)
                        .withSchema(avroSchema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withDictionaryEncoding(true)
                        .withCompressionCodec(storage.conf()
                                .parquetCompressionCodecName())
                        .withPageSize(PAGE_SIZE);
              writer = builder.build();
            }
            log.trace("Sink record with view {}: {}", recordView,
                sinkRecordToLoggableString(record));
            // Create a new GenericRecord
            GenericRecord value = new GenericData.Record(avroSchema);
            // Set values for the fields
            value.put("timestamp", record.timestamp());
            value.put("key", record.key());
            value.put("value", record.value());
            writer.write((GenericRecord) value);
          }

          @Override
          public void close() throws IOException {
            if (writer != null) {
              writer.close();
            }
          }

          @Override
          public void commit() throws IOException {
            s3ParquetOutputFile.s3out.setCommit();
            if (writer != null) {
              writer.close();
            }
          }
        }
    );
  }

  private static class S3ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;
    private S3Storage storage;
    private String filename;
    private S3ParquetOutputStream s3out;

    S3ParquetOutputFile(S3Storage storage, String filename) {
      this.storage = storage;
      this.filename = filename;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      s3out = (S3ParquetOutputStream) storage.create(filename, true, EtnetTransFormat.class);
      return s3out;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return DEFAULT_BLOCK_SIZE;
    }
  }
}
