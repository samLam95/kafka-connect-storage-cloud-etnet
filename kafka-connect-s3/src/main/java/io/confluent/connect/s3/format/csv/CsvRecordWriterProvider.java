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

package io.confluent.connect.s3.format.csv;

import static io.confluent.connect.s3.util.Utils.getAdjustedFilename;
import static io.confluent.connect.s3.util.Utils.sinkRecordToLoggableString;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.RecordViewSetter;
import io.confluent.connect.s3.format.S3RetriableRecordWriter;
import io.confluent.connect.s3.storage.IORecordWriter;
import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class CsvRecordWriterProvider extends RecordViewSetter 
    implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(CsvRecordWriterProvider.class);
  private static final String EXTENSION = ".csv";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private final S3Storage storage;

  CsvRecordWriterProvider(S3Storage storage) {
    this.storage = storage;
  }

  @Override
  public String getExtension() {
    return EXTENSION + storage.conf().getCompressionType().extension;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    return new S3RetriableRecordWriter(new IORecordWriter() {
      final String adjustedFilename = getAdjustedFilename(recordView, filename, getExtension());
      final S3OutputStream s3out = storage.create(adjustedFilename, true, CsvFormat.class);
      final OutputStream s3outWrapper = s3out.wrapForCompression();
      final PrintWriter writer = new PrintWriter(s3outWrapper);

      @Override
      public void write(SinkRecord record) throws IOException {
        log.trace("Sink record with view {}: {}", 
            recordView, sinkRecordToLoggableString(record));
        Long timestamp = record.timestamp();
        Object keyObject = record.key();
        Object valueObject = record.value();
        if (timestamp instanceof Long 
            && keyObject instanceof String && valueObject instanceof String) {
          writer.write(record.timestamp() + "," + record.key() + "," + record.value());
          writer.write(LINE_SEPARATOR);
        }
      }

      @Override
      public void commit() throws IOException {
        // Flush is required here, because closing the writer will close the underlying
        // S3
        // output stream before committing any data to S3.
        writer.flush();
        s3out.commit();
        s3outWrapper.close();
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }
    });
  }
}
