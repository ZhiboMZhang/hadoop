package org.apache.hadoop.mapred.workflow;

/**
 * Submit a workflow for deadline/budget constrained scheduling.
 */
public class WorkflowSubmitter {

	// Entry point for execution of workflow scheduling.
	public static void main(String[] args) {
		String usage = "workflow xml-conf-file";

		if (args.length != 1) {
			System.err.println(usage);
			System.exit(-1);
		}
		
		// Validate the input configuration.
		String fileName = args[0];
		WorkflowConf workflowConf = new WorkflowConf(fileName);
		
		// use code from hadoop.util.RunJar to check jar config?

	}
}
