package org.apache.drill.exec.planner.fragment;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.proto.BitControl.QueryContextInformation;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.work.QueryWorkUnit;

import java.util.Collection;

public interface QueryParallelizer extends ParallelizationParameters {

  QueryWorkUnit generateWorkUnits(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
                                  Collection<DrillbitEndpoint> activeEndpoints, Fragment rootFragment,
                                  UserSession session, QueryContextInformation queryContextInfo) throws ExecutionSetupException;
}
