package org.apache.hadoop.mapred.workflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WorkflowInfo implements Writable {

  private WorkflowID workflowId;
  private Text user;
  private Path workflowSubmitDir;

  public WorkflowInfo(WorkflowID workflowId, Text user, Path workflowSubmitDir) {
    this.workflowId = workflowId;
    this.user = user;
    this.workflowSubmitDir = workflowSubmitDir;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    workflowId.write(out);
    user.write(out);
    Text.writeString(out, workflowSubmitDir.toString());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    workflowId = new WorkflowID();
    workflowId.readFields(in);
    user = new Text();
    user.readFields(in);
    workflowSubmitDir = new Path(Text.readString(in));
  }

}