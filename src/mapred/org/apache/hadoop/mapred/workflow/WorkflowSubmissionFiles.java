package org.apache.hadoop.mapred.workflow;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

public class WorkflowSubmissionFiles {

  private final static Log LOG = LogFactory.getLog(WorkflowSubmissionFiles.class);

  private final static FsPermission WORKFLOW_DIR_PERMISSION = FsPermission
      .createImmutable((short) 0700);

  /**
   * Initializes the staging directory and returns the path. It also keeps track
   * of all necessary ownership & permissions.
   *
   * @param client The {@link WorkflowClient} use.
   * @param conf The workflow configuration.
   *
   * @return A {@link Path} representing the workflow staging directory.
   * @throws IOException
   * @throws InterruptedException
   */
  public static Path getStagingDir(WorkflowClient client, WorkflowConf conf)
      throws IOException, InterruptedException {

    Path stagingArea = client.getStagingAreaDir();
    FileSystem fileSystem = stagingArea.getFileSystem(conf);

    String realUser = UserGroupInformation.getLoginUser().getShortUserName();
    String currUser = UserGroupInformation.getCurrentUser().getShortUserName();

    if (fileSystem.exists(stagingArea)) {
      FileStatus fileSystemStatus = fileSystem.getFileStatus(stagingArea);
      String owner = fileSystemStatus.getOwner();

      if (!(owner.equals(currUser) || owner.equals(realUser))) {
        throw new IOException("The ownership on the staging directory "
            + stagingArea + " is not as expected. It is owned by " + owner
            + ". The direcotry must be owned by the submitter " + currUser
            + " or by " + realUser);
      }

      if (!fileSystemStatus.getPermission().equals(WORKFLOW_DIR_PERMISSION)) {
        LOG.info("Permissions on workflow staging directory " + stagingArea
            + " are incorrect: " + fileSystemStatus.getPermission()
            + ". Correcting permissions to " + WORKFLOW_DIR_PERMISSION);
        fileSystem.setPermission(stagingArea, WORKFLOW_DIR_PERMISSION);
      }

    } else {
      fileSystem.mkdirs(stagingArea, new FsPermission(WORKFLOW_DIR_PERMISSION));
    }
    return stagingArea;
  }

}