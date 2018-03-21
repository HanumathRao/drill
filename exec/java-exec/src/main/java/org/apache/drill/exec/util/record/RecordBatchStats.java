/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.util.record;

import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentContextImpl;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Utility class to capture key record batch statistics.
 */
public final class RecordBatchStats {
  /** A prefix for all batch stats to simplify search */
  public static final String BATCH_STATS_PREFIX = "BATCH_STATS";

  /** Helper class which loads contextual record batch logging options */
  public static final class RecordBatchStatsContext {
    /** batch size logging for all readers */
    private final boolean enableBatchSzLogging;
    /** Fine grained batch size logging */
    private final boolean enableFgBatchSzLogging;
    /** Unique Operator Identifier */
    private final String contextOperatorId;

    /**
     * @param options options manager
     */
    public RecordBatchStatsContext(FragmentContext context, OperatorContext oContext) {
      enableBatchSzLogging   = context.getOptions().getOption(ExecConstants.STATS_LOGGING_BATCH_SZ_OPTION).bool_val;
      enableFgBatchSzLogging = context.getOptions().getOption(ExecConstants.STATS_LOGGING_FG_BATCH_SZ_OPTION).bool_val;
      contextOperatorId      = new StringBuilder()
        .append(getQueryId(context))
        .append(":")
        .append(oContext.getStats().getId())
        .toString();
    }

    /**
     * @return the enableBatchSzLogging
     */
    public boolean isEnableBatchSzLogging() {
      return enableBatchSzLogging || enableFgBatchSzLogging;
    }

    /**
     * @return the enableFgBatchSzLogging
     */
    public boolean isEnableFgBatchSzLogging() {
      return enableFgBatchSzLogging;
    }

    /**
     * @return the contextOperatorId
     */
    public String getContextOperatorId() {
      return contextOperatorId;
    }

    private String getQueryId(FragmentContext _context) {
      if (_context instanceof FragmentContextImpl) {
        final FragmentContextImpl context = (FragmentContextImpl) _context;
        final FragmentHandle handle       = context.getHandle();

        if (handle != null) {
          return QueryIdHelper.getQueryIdentifier(handle);
        }
      }
      return "NA";
    }

  }

  /**
   * Prints batch count and memory statistics for the input record batch
   *
   * @param stats instance identifier
   * @param sourceId optional source identifier for scanners
   * @param recordBatch a set of records
   * @param verbose whether to include fine-grained stats
   *
   * @return a string containing the record batch statistics
   */
  public static String printRecordBatchStats(String statsId,
    String sourceId,
    Map<String, ValueVector> recordBatch,
    boolean verbose) {

    StringBuilder aggregateMsg = new StringBuilder(256);
    StringBuilder fieldsMsg    = new StringBuilder(256);
    int batchSize              = recordBatch.values().iterator().next().getAccessor().getValueCount();
    long totalAllocSize        = 0;
    long totalBufferSize       = 0;
    long totalPayloadByte      = 0;

    if (verbose) {
      // Print the stats headers
      printValueVectorFieldsStatsHeaders(fieldsMsg);
    }

    for (Map.Entry<String, ValueVector> entry : recordBatch.entrySet()) {
      final String fieldName = entry.getKey();
      final ValueVector v    = entry.getValue();

      if (verbose) {
        printFieldStats(statsId, fieldName, v, fieldsMsg);
      }

      // Aggregate total counter stats
      totalAllocSize   += v.getAllocatedSize();
      totalBufferSize  += v.getBufferSize();
      totalPayloadByte += v.getPayloadByteCount(batchSize);
    }

    // Prints the aggregate record batch statistics
    printAggregateStats(statsId,
      sourceId,
      batchSize,
      totalAllocSize,
      totalBufferSize,
      totalPayloadByte,
      aggregateMsg);

    return aggregateMsg.toString() + fieldsMsg.toString();
  }

  /**
   * Prints a materialized field type
   * @param field materialized field
   * @param msg string builder where to append the field type
   */
  public static void printType(MaterializedField field, StringBuilder msg) {
    final MajorType type = field.getType();

    msg.append(type.getMinorType().name());
    msg.append(':');
    msg.append(type.getMode().name());
  }

  /**
   * @param allocator dumps allocator statistics
   * @return string with allocator statistics
   */
  public static String printAllocatorStats(BufferAllocator allocator) {
    StringBuilder msg = new StringBuilder();
    msg.append(BATCH_STATS_PREFIX);
    msg.append(": dumping allocator statistics:\n");
    msg.append(BATCH_STATS_PREFIX);
    msg.append(": ");
    msg.append(allocator.toString());

    return msg.toString();
  }

  private static void printValueVectorFieldsStatsHeaders(StringBuilder msg) {
    msg.append(BATCH_STATS_PREFIX);
    msg.append("\tOriginator\tName\tType\tAlloc-Sz\tUsed-Sz\tAvgPay-Sz\n");
  }

  private static void printFieldStats(String statsId,
    String fieldName,
    ValueVector v,
    StringBuilder msg) {

    final MaterializedField field = v.getField();
    final int batchSize           = v.getAccessor().getValueCount();

    msg.append(BATCH_STATS_PREFIX);
    msg.append("\t");
    msg.append(statsId);
    msg.append('\t');
    msg.append(fieldName);
    msg.append('\t');
    printType(field, msg);
    msg.append('\t');
    msg.append(v.getAllocatedSize());
    msg.append('\t');
    msg.append(v.getBufferSize());
    msg.append('\t');
    if (batchSize > 0) {
      msg.append(v.getPayloadByteCount(batchSize) / batchSize);
    } else {
      msg.append("NA");
    }
    msg.append('\n');
  }

  private static void printAggregateStats(String statsId,
    String sourceId,
    int batchSize,
    long totalAllocSize,
    long totalBufferSize,
    long totalPayloadByte,
    StringBuilder msg) {

    msg.append(BATCH_STATS_PREFIX);
    msg.append(": Originator: [");
    msg.append(statsId);
    msg.append(':');
    msg.append(sourceId);
    msg.append("], Num Recs: [");
    msg.append(batchSize);
    msg.append("], Total Allocated Size: [");
    msg.append(totalAllocSize);
    msg.append("], Total Buffer Size: [");
    msg.append(totalBufferSize);
    msg.append("], Total Payload Byte: [");
    msg.append(totalPayloadByte);
    msg.append("]\n");

  }

  /**
   * Disabling class object instantiation.
   */
  private RecordBatchStats() {
  }

}