/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.mapred.workflow;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A utility to manage workflow submission files.
 *
 * Note that this class is for framework internal usage only.
 */
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