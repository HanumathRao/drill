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
package org.apache.drill.exec.physical.impl.mergereceiver;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergingReceiverPOP;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.BatchProvider;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;

/**
 * The MergingRecordBatch merges pre-sorted record batches from remote senders.
 */
public class MergingLocalRecordBatch extends MergingRecordBatch<MergingReceiverPOP, RecordBatch> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingRecordBatch.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(MergingRecordBatch.class);

  public MergingLocalRecordBatch(final FragmentContext context,
                                  final MergingReceiverPOP config,
                                  final BatchProvider<RecordBatch>[] fragProviders) {
    super(context, config, fragProviders);
  }

  @Override
  protected RecordBatch[] allocateFragmentBatches(int senderCount) {
    return new RecordBatch[senderCount];
  }
}