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

package io.confluent.connect.hdfs.sequence;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Vipin Kumar
 */
public class SequenceFileWriterProvider implements RecordWriterProvider<HdfsSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(SequenceFileWriterProvider.class);

  private final HdfsStorage storage;
  private boolean isAppended;

  /**
   * @param storage    HDFS Storage Object
   * @param isAppended If Sequence file needs to be open in append mode or not
   */
  public SequenceFileWriterProvider(HdfsStorage storage, boolean isAppended) {

    this.storage = storage;
    this.isAppended = isAppended;
  }

  @Override
  public String getExtension() {
    return "";
  }

  @Override
  public RecordWriter getRecordWriter(final HdfsSinkConnectorConfig hdfsSinkConnectorConfig,
                                      final String filename) {
    SequenceFile.Writer writer = null;
    try {
      Path path = new Path(filename);
      SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(path);
      SequenceFile.Writer.Option optKey =
          SequenceFile.Writer.keyClass(LongWritable.class);
      SequenceFile.Writer.Option optVal =
          SequenceFile.Writer.valueClass(BytesWritable.class);
      SequenceFile.Writer.Option optCodec =
          SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new BZip2Codec());
      writer = SequenceFile.createWriter(hdfsSinkConnectorConfig.getHadoopConfiguration(),
          optPath, optKey, optVal, SequenceFile.Writer.appendIfExists(isAppended), optCodec);

    } catch (IOException e) {
      throw new ConnectException(e);
    }
    SequenceFile.Writer finalWriter = writer;
    return new RecordWriter() {
      @Override
      public void write(SinkRecord record) {
        if (record != null) {
          byte[] value = (byte[]) record.value();
          BytesWritable bytesObject = new BytesWritable();
          bytesObject.set(value, 0, value.length);
          try {
            finalWriter.append(new LongWritable(System.currentTimeMillis()), bytesObject);
            finalWriter.sync();
          } catch (IOException e) {
            log.error("error during Sequence file sync ");
            throw new ConnectException(e);
          }
        }
      }

      @Override
      public void close() {
        try {
          finalWriter.close();
        } catch (IOException e) {
          log.error("error during close");
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {

      }
    };
  }

}
