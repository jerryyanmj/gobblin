/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

/**
 * A central place for configuration related constants of Gobblin on Yarn.
 *
 * @author ynli
 */
public class ConfigurationConstants {

  public static final String GOBBLIN_YARN_PREFIX = "gobblin.yarn.";

  /**
   * General Gobblin Yarn application configuration properties.
   */
  public static final String APPLICATION_NAME_KEY = GOBBLIN_YARN_PREFIX + "app.name";
  public static final String APPLICATION_NAME_OPTION_NAME = "app_name";
  public static final String DEFAULT_APPLICATION_NAME = "GobblinYarn";
  public static final String APP_QUEUE_KEY = GOBBLIN_YARN_PREFIX + "app.queue";
  public static final String DEFAULT_APP_QUEUE = "default";

  /**
   * Gobblin Yarn ApplicationMaster configuration properties.
   */
  public static final String APP_MASTER_MEMORY_MBS_KEY = GOBBLIN_YARN_PREFIX + "app.master.memory.mbs";
  public static final int DEFAULT_APP_MASTER_MEMORY_MBS = 256;
  public static final String APP_MASTER_CORES_KEY = GOBBLIN_YARN_PREFIX + "app.master.cores";
  public static final int DEFAULT_APP_MASTER_CORES = 1;
  public static final String APP_MASTER_JARS_KEY = GOBBLIN_YARN_PREFIX + "app.master.jars";
  public static final String APP_MASTER_FILES_LOCAL_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.local";
  public static final String APP_MASTER_FILES_REMOTE_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.remote";
  public static final String APP_MASTER_WORK_DIR_NAME = "appmaster";

  /**
   * Gobblin Yarn container configuration properties.
   */
  public static final String INITIAL_CONTAINERS_KEY = GOBBLIN_YARN_PREFIX + "initial.containers";
  public static final int DEFAULT_INITIAL_CONTAINERS = 1;
  public static final String CONTAINER_MEMORY_MBS_KEY = GOBBLIN_YARN_PREFIX + "container.memory.mbs";
  public static final int DEFAULT_CONTAINER_MEMORY_MBS = 256;
  public static final String CONTAINER_CORES_KEY = GOBBLIN_YARN_PREFIX + "container.cores";
  public static final int DEFAULT_CONTAINER_CORES = 1;
  public static final String CONTAINER_JARS_KEY = GOBBLIN_YARN_PREFIX + "app.master.jars";
  public static final String CONTAINER_FILES_LOCAL_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.local";
  public static final String CONTAINER_FILES_REMOTE_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.remote";
  public static final String CONTAINER_WORK_DIR_NAME = "container";

  /**
   * Helix configuration properties.
   */
  public static final String HELIX_CLUSTER_NAME_KEY = GOBBLIN_YARN_PREFIX + "helix.cluster.name";
  public static final String ZK_CONNECTION_STRING_KEY = GOBBLIN_YARN_PREFIX + "zk.connection.string";
  public static final String DEFAULT_ZK_CONNECTION_STRING = "localhost:2181";
  public static final String WORK_UNIT_FILE_PATH = GOBBLIN_YARN_PREFIX + "work.unit.file.path";

  /**
   * Security and authentication configuration properties.
   */
  public static final String KEYTAB_FILE_PATH = GOBBLIN_YARN_PREFIX + "keytab.file.path";
  public static final String KEYTAB_PRINCIPAL_NAME = GOBBLIN_YARN_PREFIX + "keytab.principal.name";
  public static final String TOKEN_FILE_PATH = GOBBLIN_YARN_PREFIX + "token.file.path";
  public static final String LOGIN_INTERVAL_IN_HOURS = GOBBLIN_YARN_PREFIX + "login.interval.hours";
  public static final String TOKEN_RENEW_INTERVAL_IN_HOURS = GOBBLIN_YARN_PREFIX + "token.renew.interval.hours";

  /**
   * Resource/dependencies configuration properties.
   */
  public static final String LIB_JARS_DIR_KEY = GOBBLIN_YARN_PREFIX + "lib.jars.dir";
  public static final String JOB_CONF_PACKAGE_PATH_KEY = GOBBLIN_YARN_PREFIX + "job.conf.package.path";
  public static final String LIB_JARS_DIR_NAME = "_libjars";
  public static final String APP_JARS_DIR_NAME = "_appjars";
  public static final String APP_FILES_DIR_NAME = "_appfiles";
  public static final String INPUT_WORK_UNIT_DIR_NAME = "_workunits";
  public static final String OUTPUT_TASK_STATE_DIR_NAME = "_taskstates";
}
