/**
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
package org.apache.drill.exec.record;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.rpc.data.AckSender;
import java.util.List;

public interface FragmentBatchWrapper {
  int getRecordCount();

  int getFieldCount();

  List<SerializedField> getFieldList();

  @Override
  String toString();

  void release();

  AckSender getSender();

  void sendOk();

  long getByteCount();

  boolean isAckSent();

  <BL extends BatchLoader> BL getBatchLoader(BufferAllocator allocator);

  <FB extends FragmentBatch> FB getEmptyBatch(FragmentRecordBatch header, DrillBuf body, AckSender sender);

  FragmentBatch getBatch();
}
