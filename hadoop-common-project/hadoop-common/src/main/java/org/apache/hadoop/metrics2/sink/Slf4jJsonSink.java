/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.sink;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * A metrics sink that writes to a slf4j with json
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Slf4jJsonSink implements MetricsSink, Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(Slf4jJsonSink.class);
  private static final ThreadLocal<StringBuilder> STRING_BUILDER =
      new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  @Override
  public void init(SubsetConfiguration conf) {

  }
  @Override
  public void putMetrics(MetricsRecord record) {
    StringBuilder sb = STRING_BUILDER.get();
    String separator = ": ";
    sb.append("{");
    sb.append("\"");
    sb.append("timestamp");
    sb.append("\"");
    sb.append(separator);
    sb.append("\"");
    sb.append(record.timestamp());
    sb.append("\"");
    sb.append(",");
    sb.append("\"");
    sb.append(record.name());
    sb.append("\"");
    sb.append(separator);
    sb.append("{");
    for (MetricsTag tag : record.tags()) {
      sb.append("\"");
      sb.append(tag.name());
      sb.append("\"");
      sb.append(":");
      if (tag.value() != null && tag.value().contains("{")) {
        sb.append(tag.value());
      } else {
        sb.append("\"");
        sb.append(tag.value());
        sb.append("\"");
      }
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    if (record.metrics().iterator().hasNext()) {
      sb.append(",");
      for (AbstractMetric metric : record.metrics()) {
        sb.append("\"");
        sb.append(metric.name());
        sb.append("\"");
        sb.append(":");
        sb.append(metric.value());
        sb.append(",");
      }
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append("}");
    sb.append("}");
    flush();
  }

  @Override
  public void flush() {
//    if(STRING_BUILDER.get().length()>0){
//      LOG.info(STRING_BUILDER.get().toString());
//    }
    LOG.info(STRING_BUILDER.get().toString());
    STRING_BUILDER.get().setLength(0);
  }

  @Override
  public void close() throws IOException {
    STRING_BUILDER.remove();
  }
}
