package org.apache.drill.exec.planner.cost;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("cost-estimates")
public class PrelCostEstimates {
  private final double memoryCost;
  private final double outputRowCount;

  public static PrelCostEstimates ZERO_COST = new PrelCostEstimates(0,0);

  public PrelCostEstimates(@JsonProperty("memoryCost") double memory,
                           @JsonProperty("outputRowCount") double rowCount) {
    this.memoryCost = memory;
    this.outputRowCount = rowCount;
  }

  public double getOutputRowCount() {
    return outputRowCount;
  }

  public double getMemoryCost() {
    return memoryCost;
  }
}