/*-
 * -\-\-
 * Flo Styx
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.contrib.styx;

final class Constants {

  Constants() {
    throw new UnsupportedOperationException();
  }

  public static final String STYX_TERMINATION_LOG = "styx.termination.log";
  public static final String STYX_COMPONENT_ID = "styx.component.id";
  public static final String STYX_WORKFLOW_ID = "styx.workflow.id";
  public static final String STYX_PARAMETER = "styx.parameter";
  public static final String STYX_EXECUTION_ID = "styx.execution.id";
  public static final String STYX_TRIGGER_ID = "styx.trigger.id";
  public static final String STYX_TRIGGER_TYPE = "styx.trigger.type";
}
