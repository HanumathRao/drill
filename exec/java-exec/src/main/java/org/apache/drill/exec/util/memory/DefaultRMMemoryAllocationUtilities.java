package org.apache.drill.exec.util.memory;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.foreman.rm.ThrottledResourceManager;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DefaultRMMemoryAllocationUtilities {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultRMMemoryAllocationUtilities.class);

  public static void planMemory(QueryContext queryContext, QueryWorkUnit work, PhysicalPlan plan, ThrottledResourceManager rm) {
    if (plan.getProperties().hasResourcePlan) {
      logger.debug("Memory already planned.");
      return;
    }

    // Group fragments by node.

    Map<String, Collection<PhysicalOperator>> nodeMap = buildBufferedOpMap(work);

    // Memory must be symmetric to avoid bottlenecks in which one node has
    // sorts (say) with less memory than another, causing skew in data arrival
    // rates for downstream operators.

    int width = countBufferingOperators(nodeMap);

    // Then, share memory evenly across the
    // all sort operators on that node. This handles asymmetric distribution
    // such as occurs if a sort appears in the root fragment (the one with
    // screen),
    // which is never parallelized.

    for (Map.Entry<String, Collection<PhysicalOperator>> entry : nodeMap.entrySet()) {
      planNodeMemory(entry.getKey(), rm.defaultQueryMemoryPerNode(plan.totalCost()), queryContext, entry.getValue(), width, rm);
    }
  }

  public static void getBufferedOps(Multimap<String, PhysicalOperator> map,
                                    QueryWorkUnit.MinorFragmentDefn defn) {
    List<PhysicalOperator> bufferedOps = getBufferedOps(defn.root());
    if (!bufferedOps.isEmpty()) {
      map.putAll(defn.fragment().getAssignment().getAddress(), bufferedOps);
    }
  }

  public static class BufferedOpFinder extends AbstractPhysicalVisitor<Void, List<PhysicalOperator>, RuntimeException> {
    @Override
    public Void visitOp(PhysicalOperator op, List<PhysicalOperator> value)
      throws RuntimeException {
      if (op.isBufferedOperator(null)) {
        value.add(op);
      }
      visitChildren(op, value);
      return null;
    }
  }

  /**
   * Search an individual fragment tree to find any buffered operators it may
   * contain.
   *
   * @param root
   * @return
   */

  private static List<PhysicalOperator> getBufferedOps(FragmentRoot root) {
    List<PhysicalOperator> bufferedOps = new ArrayList<>();
    BufferedOpFinder finder = new BufferedOpFinder();
    root.accept(finder, bufferedOps);
    return bufferedOps;
  }

  /**
   * Build a list of external sorts grouped by node. We start with a list of
   * minor fragments, each with an endpoint (node). Multiple minor fragments
   * may appear on each node, and each minor fragment may have 0, 1 or more
   * sorts.
   *
   * @return
   */

  private static Map<String, Collection<PhysicalOperator>> buildBufferedOpMap(QueryWorkUnit work) {
    Multimap<String, PhysicalOperator> map = ArrayListMultimap.create();
    getBufferedOps(map, work.getRootFragmentDefn());
    for (QueryWorkUnit.MinorFragmentDefn defn : work.getMinorFragmentDefns()) {
      getBufferedOps(map, defn);
    }
    return map.asMap();
  }

  private static int countBufferingOperators(Map<String, Collection<PhysicalOperator>> nodeMap) {
    int width = 0;
    for (Collection<PhysicalOperator> fragSorts : nodeMap.values()) {
      width = Math.max(width, fragSorts.size());
    }
    return width;
  }

  /**
   * Given the set of buffered operators (from any number of fragments) on a
   * single node, shared the per-query memory equally across all the
   * operators.
   *
   * @param nodeAddr
   * @param bufferedOps
   * @param width
   */

  private static void planNodeMemory(String nodeAddr, long queryMemoryPerNode, QueryContext queryContext,
                                     Collection<PhysicalOperator> bufferedOps, int width, ThrottledResourceManager rm) {

    // If no buffering operators, nothing to plan.

    if (bufferedOps.isEmpty()) {
      return;
    }

    // Divide node memory evenly among the set of operators, in any minor
    // fragment, on the node. This is not very sophisticated: it does not
    // deal with, say, three stacked sorts in which, if sort A runs, then
    // B may be using memory, but C cannot be active. That kind of analysis
    // is left as a later exercise.

    // Set a floor on the amount of memory per operator based on the
    // configured minimum. This is likely unhelpful because we are trying
    // to work around constrained memory by assuming more than we actually
    // have. This may lead to an OOM at run time.

    long preferredOpMemory = queryMemoryPerNode / width;
    long perOpMemory = Math.max(preferredOpMemory, rm.minimumOperatorMemory());
    if (preferredOpMemory < perOpMemory) {
      logger.warn("Preferred per-operator memory: {}, actual amount: {}",
        preferredOpMemory, perOpMemory);
    }
    logger.debug(
      "Query: {}, Node: {}, allocating {} bytes each for {} buffered operator(s).",
      QueryIdHelper.getQueryId(queryContext.getQueryId()), nodeAddr,
      perOpMemory, width);

    for (PhysicalOperator op : bufferedOps) {

      // Limit the memory to the maximum in the plan. Doing so is
      // likely unnecessary, and perhaps harmful, because the pre-planned
      // allocation is the default maximum hard-coded to 10 GB. This means
      // that even if 20 GB is available to the sort, it won't use more
      // than 10GB. This is probably more of a bug than a feature.

      long alloc = Math.min(perOpMemory, op.getMaxAllocation());

      // Place a floor on the memory that is the initial allocation,
      // since we don't want the operator to run out of memory when it
      // first starts.

      alloc = Math.max(alloc, op.getInitialAllocation());

      if (alloc > preferredOpMemory && alloc != perOpMemory) {
        logger.warn("Allocated memory of {} for {} exceeds available memory of {} " +
            "due to operator minimum",
          alloc, op.getClass().getSimpleName(), preferredOpMemory);
      }
      else if (alloc < preferredOpMemory) {
        logger.warn("Allocated memory of {} for {} is less than available memory " +
            "of {} due to operator limit",
          alloc, op.getClass().getSimpleName(), preferredOpMemory);
      }
      op.setMaxAllocation(alloc);
    }
  }
}
