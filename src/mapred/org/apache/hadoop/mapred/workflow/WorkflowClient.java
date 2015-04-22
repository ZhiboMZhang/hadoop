package org.apache.hadoop.mapred.workflow;

import java.io.IOException;

/**
 * <code>WorkflowClient</code> is the primary interface for the user-workflow
 * to interact with the {@link JobTracker}.
 */
// See JobClient.
public class WorkflowClient {

	
	public static final String MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_KEY = "";
	public static final Boolean MAPREDUCE_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = false;
	public static final String MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_KEY = "";
	public static final String MAPREDUCE_CLIENT_RETRY_POLICY_SPEC_DEFAULT = "";
	
	
	
	public static RunningWorkflow runWorkflow(WorkflowConf workflow) throws IOException {
		return null;
	}
}
