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

package org.apache.drill.exec.planner.index.rules;

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.physical.base.DbGroupScan;
import org.apache.drill.exec.planner.index.FunctionalIndexInfo;
import org.apache.drill.exec.planner.index.IndexCollection;
import org.apache.drill.exec.planner.index.IndexConditionInfo;
import org.apache.drill.exec.planner.index.IndexDescriptor;
import org.apache.drill.exec.planner.index.IndexLogicalPlanCallContext;
import org.apache.drill.exec.planner.index.IndexPlanUtils;
import org.apache.drill.exec.planner.index.IndexSelector;
import org.apache.drill.exec.planner.index.IndexableExprMarker;
import org.apache.drill.exec.planner.index.Statistics;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prule;
import org.slf4j.Logger;


public abstract class AbstractIndexPrule extends Prule {

  public AbstractIndexPrule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  /**
   * Return the index collection relevant for the underlying data source
   * @param settings
   * @param scan
   */
  protected IndexCollection getIndexCollection(PlannerSettings settings, DrillScanRel scan) {
    DbGroupScan groupScan = (DbGroupScan)scan.getGroupScan();
    return groupScan.getSecondaryIndexCollection(scan);
  }

  protected boolean analyzeCondition(IndexLogicalPlanCallContext indexContext,
      IndexCollection collection,
      RexNode condition,
      RexBuilder rexBuilder,
      IndexConditionInfo.Builder infoBuilder,
      Logger logger) {
    if (! (indexContext.scan.getGroupScan() instanceof DbGroupScan) ) {
      return false;
    }

    IndexConditionInfo cInfo = infoBuilder.getCollectiveInfo(indexContext);
    boolean isValidIndexHint = infoBuilder.isValidIndexHint(indexContext);

    if (!cInfo.hasIndexCol) {
      logger.info("index_plan_info: No index columns are projected from the scan..continue.");
      return false;
    }

    if (cInfo.indexCondition == null) {
      logger.info("index_plan_info: No conditions were found eligible for applying index lookup.");
      return false;
    }

    if (!indexContext.indexHint.equals("") && !isValidIndexHint) {
      logger.warn("index_plan_info: Index Hint {} is not useful as index with that name is not available", indexContext.indexHint);
    }

    RexNode indexCondition = cInfo.indexCondition;
    RexNode remainderCondition = cInfo.remainderCondition;

    if (remainderCondition.isAlwaysTrue()) {
      remainderCondition = null;
    }
    logger.debug("index_plan_info: condition split into indexcondition: {} and remaindercondition: {}", indexCondition, remainderCondition);

    IndexableExprMarker indexableExprMarker = new IndexableExprMarker(indexContext.scan);
    indexCondition.accept(indexableExprMarker);
    indexContext.origMarker = indexableExprMarker;

    // save the information in the index context for future use
    indexContext.indexCondition = indexCondition;
    indexContext.remainderCondition = remainderCondition;
    indexContext.isValidIndexHint = isValidIndexHint;

    return true;
  }

  protected boolean initializeStatistics(DrillScanRel scan,
      PlannerSettings settings,
      IndexLogicalPlanCallContext indexContext,
      RexNode condition,
      boolean isValidIndexHint,
      Logger logger) {
    double totalRows = 0;
    double filterRows = totalRows;
    if (scan.getGroupScan() instanceof DbGroupScan) {
      // Initialize statistics
      DbGroupScan dbScan = ((DbGroupScan) scan.getGroupScan());
      if (settings.isStatisticsEnabled()) {
        dbScan.getStatistics().initialize(condition, scan, indexContext);
      }
      totalRows = dbScan.getRowCount(null, scan);
      filterRows = dbScan.getRowCount(condition, scan);
      double sel = filterRows/totalRows;
      if (totalRows != Statistics.ROWCOUNT_UNKNOWN &&
          filterRows != Statistics.ROWCOUNT_UNKNOWN &&
          !settings.isDisableFullTableScan() && !isValidIndexHint &&
          sel > Math.max(settings.getIndexCoveringSelThreshold(),
              settings.getIndexNonCoveringSelThreshold() )) {
        // If full table scan is not disabled, generate full table scan only plans if selectivity
        // is greater than covering and non-covering selectivity thresholds
        logger.info("index_plan_info: Skip index planning because filter selectivity: {} is greater than thresholds {}, {}",
            sel, settings.getIndexCoveringSelThreshold(), settings.getIndexNonCoveringSelThreshold());
        return false;
      }
    }

    if (totalRows == Statistics.ROWCOUNT_UNKNOWN ||
        totalRows == 0 || filterRows == Statistics.ROWCOUNT_UNKNOWN ) {
      logger.warn("index_plan_info: Total row count is UNKNOWN or 0, or filterRows UNKNOWN; skip index planning");
      return false;
    }

    indexContext.totalRows = totalRows;
    indexContext.filterRows = filterRows;

    return true;
  }

  protected IndexSelector createAndInitSelector(IndexLogicalPlanCallContext indexContext,
      IndexCollection collection,
      RexBuilder builder,
      Logger logger
      ) {
    IndexSelector selector = new IndexSelector(indexContext.indexCondition,
        indexContext.remainderCondition,
        indexContext,
        collection,
        builder,
        indexContext.totalRows);

    for (IndexDescriptor indexDesc : collection) {
      logger.info("index_plan_info indexDescriptor: {}", indexDesc.toString());
      // check if any of the indexed fields of the index are present in the filter condition
      if (IndexPlanUtils.conditionIndexed(indexContext.getOrigMarker(), indexDesc) != IndexPlanUtils.ConditionIndexed.NONE) {
        if (indexContext.isValidIndexHint && !indexContext.indexHint.equals(indexDesc.getIndexName())) {
          logger.info("index_plan_info: Index {} is being discarded due to index Hint", indexDesc.getIndexName());
          continue;
        }
        FunctionalIndexInfo functionInfo = indexDesc.getFunctionalInfo();
        selector.addIndex(indexDesc, IndexPlanUtils.isCoveringIndex(indexContext, functionInfo),
            indexContext.lowerProject != null ? indexContext.lowerProject.getRowType().getFieldCount() :
                indexContext.scan.getRowType().getFieldCount());
      }
    }
    return selector;
  }

}
