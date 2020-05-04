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
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.apache.hadoop.fs.Path;

/**
 * @author Vipin Kumar
 */
public class SequenceFormat implements Format<HdfsSinkConnectorConfig, Path> {
  private final HdfsStorage storage;
  private boolean isAppended;

  public SequenceFormat(HdfsStorage storage, boolean isAppended) {
    this.storage = storage;
    this.isAppended = isAppended;
  }

  @Override
  public RecordWriterProvider<HdfsSinkConnectorConfig> getRecordWriterProvider() {
    return new SequenceFileWriterProvider(storage, isAppended);
  }

  @Override
  public SchemaFileReader<HdfsSinkConnectorConfig, Path> getSchemaFileReader() {
    return new SequenceFileReader();
  }

  @Override
  public Object getHiveFactory() {
    throw new UnsupportedOperationException(
        "Hive integration is not currently supported in sequence  format");
  }
}
