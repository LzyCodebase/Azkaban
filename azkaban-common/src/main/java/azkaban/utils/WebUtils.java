/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.utils;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.Status;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;

public class WebUtils {

  public static final String DATE_TIME_STRING = "YYYY-MM-dd HH:mm:ss";
  public static final String X_FORWARDED_FOR_HEADER = "X-Forwarded-For";
  private static final long ONE_KB = 1024;
  private static final long ONE_MB = 1024 * ONE_KB;
  private static final long ONE_GB = 1024 * ONE_MB;
  private static final long ONE_TB = 1024 * ONE_GB;

  public String formatDate(final long timeMS) {
    if (timeMS == -1) {
      return "-";
    }

    return DateTimeFormat.forPattern(DATE_TIME_STRING).print(timeMS);
  }

  public long currentTimestamp() {
    return System.currentTimeMillis();
  }

  public String formatDuration(final long startTime, final long endTime) {
    if (startTime == -1) {
      return "-";
    }

    final long durationMS;
    if (endTime == -1) {
      durationMS = System.currentTimeMillis() - startTime;
    } else {
      durationMS = endTime - startTime;
    }

    long seconds = durationMS / 1000;
    if (seconds < 60) {
      return seconds + " sec";
    }

    long minutes = seconds / 60;
    seconds %= 60;
    if (minutes < 60) {
      return minutes + "m " + seconds + "s";
    }

    long hours = minutes / 60;
    minutes %= 60;
    if (hours < 24) {
      return hours + "h " + minutes + "m " + seconds + "s";
    }

    final long days = hours / 24;
    hours %= 24;
    return days + "d " + hours + "h " + minutes + "m";
  }

  public String formatStatus(final Status status) {
    switch (status) {
      case SUCCEEDED:
        return "Success";
      case FAILED:
        return "Failed";
      case RUNNING:
        return "Running";
      case DISABLED:
        return "Disabled";
      case KILLED:
        return "Killed";
      case FAILED_FINISHING:
        return "Running w/Failure";
      case PREPARING:
        return "Preparing";
      case READY:
        return "Ready";
      case PAUSED:
        return "Paused";
      case SKIPPED:
        return "Skipped";
      case KILLING:
        return "Killing";
      default:
    }
    return "Unknown";
  }

  public String formatDateTime(final DateTime dt) {
    return DateTimeFormat.forPattern(DATE_TIME_STRING).print(dt);
  }

  public String formatDateTime(final long timestamp) {
    return formatDateTime(new DateTime(timestamp));
  }

  public String formatPeriod(final ReadablePeriod period) {
    String periodStr = "null";

    if (period == null) {
      return periodStr;
    }

    if (period.get(DurationFieldType.years()) > 0) {
      final int years = period.get(DurationFieldType.years());
      periodStr = years + " year(s)";
    } else if (period.get(DurationFieldType.months()) > 0) {
      final int months = period.get(DurationFieldType.months());
      periodStr = months + " month(s)";
    } else if (period.get(DurationFieldType.weeks()) > 0) {
      final int weeks = period.get(DurationFieldType.weeks());
      periodStr = weeks + " week(s)";
    } else if (period.get(DurationFieldType.days()) > 0) {
      final int days = period.get(DurationFieldType.days());
      periodStr = days + " day(s)";
    } else if (period.get(DurationFieldType.hours()) > 0) {
      final int hours = period.get(DurationFieldType.hours());
      periodStr = hours + " hour(s)";
    } else if (period.get(DurationFieldType.minutes()) > 0) {
      final int minutes = period.get(DurationFieldType.minutes());
      periodStr = minutes + " minute(s)";
    } else if (period.get(DurationFieldType.seconds()) > 0) {
      final int seconds = period.get(DurationFieldType.seconds());
      periodStr = seconds + " second(s)";
    }

    return periodStr;
  }

  public String extractNumericalId(final String execId) {
    final int index = execId.indexOf('.');
    final int index2 = execId.indexOf('.', index + 1);

    return execId.substring(0, index2);
  }

  public String displayBytes(final long sizeBytes) {
    final NumberFormat nf = NumberFormat.getInstance();
    nf.setMaximumFractionDigits(2);
    if (sizeBytes >= ONE_TB) {
      return nf.format(sizeBytes / (double) ONE_TB) + " tb";
    } else if (sizeBytes >= ONE_GB) {
      return nf.format(sizeBytes / (double) ONE_GB) + " gb";
    } else if (sizeBytes >= ONE_MB) {
      return nf.format(sizeBytes / (double) ONE_MB) + " mb";
    } else if (sizeBytes >= ONE_KB) {
      return nf.format(sizeBytes / (double) ONE_KB) + " kb";
    } else {
      return sizeBytes + " B";
    }
  }

  /**
   * Gets the actual client IP address inspecting the X-Forwarded-For HTTP header or using the
   * provided 'remote IP address' from the low level TCP connection from the client.
   *
   * If multiple IP addresses are provided in the X-Forwarded-For header then the first one (first
   * hop) is used
   *
   * @param httpHeaders List of HTTP headers for the current request
   * @param remoteAddr The client IP address and port from the current request's TCP connection
   * @return The actual client IP address
   */
  public String getRealClientIpAddr(final Map<String, String> httpHeaders,
      final String remoteAddr) {

    // If some upstream device added an X-Forwarded-For header
    // use it for the client ip
    // This will support scenarios where load balancers or gateways
    // front the Azkaban web server and a changing Ip address invalidates
    // the session

    String clientIp = httpHeaders.getOrDefault(X_FORWARDED_FOR_HEADER, null);
    if (clientIp == null) {
      clientIp = remoteAddr;
    } else {
      // header can contain comma separated list of upstream servers - get the first one
      final String[] ips = clientIp.split(",");
      clientIp = ips[0];
    }

    // Strip off port and only get IP address
    final String[] parts = clientIp.split(":");
    clientIp = parts[0];

    return clientIp;
  }
  /**
   * get cycle date from a flow
   * @param flow
   * @return the data time of the task
   */
  public String getCycleDate(ExecutableFlow flow){
    final String[] cycleDate = {"-1"};
    ExecutionOptions executionOptions = flow.getExecutionOptions();
    Map<String, String> parameters = executionOptions.getFlowParameters();
    parameters.forEach((k,v)->{
      if("cycle_date".equals(k)){
        cycleDate[0] = v;
      }
    });
    return cycleDate[0];
  }
  public String getAction(ExecutableFlow flow){
    int executionId = flow.getExecutionId();
    Status status  =  flow.getStatus();

    final String REDO = "<button type=\"button\" id=\"redo-btn\" onclick=\"redo("+executionId+")\">redo</button>";
    final String KILL = "";
    String  action = "";
    switch (status) {
      case SUCCEEDED:
        action = REDO;
        break;
      case FAILED:
        action = REDO;
        break;
      case RUNNING:
        action = KILL;
        break;
      case DISABLED:
        action = REDO;
        break;
      case KILLED:
        action = REDO;
        break;
      case FAILED_FINISHING:
        action = REDO;
        break;
      default:
    }
    return action;
  }


  public Map<String,Object> getExecutionOptionData(ExecutableFlow flow){
    Map<String,Object> executingData = new HashMap<>();
    ExecutionOptions executionOptions = flow.getExecutionOptions();
    int executionId = flow.getExecutionId();
    executingData.put("failureAction"+executionId,executionOptions.getFailureAction());
    executingData.put("failureEmails"+executionId,executionOptions.getFailureEmails());
    executingData.put("successEmails"+executionId,executionOptions.getSuccessEmails());
    executingData.put("notifyFailureFirst"+executionId,executionOptions.getNotifyOnFirstFailure());
    executingData.put("notifyFailureLast"+executionId,executionOptions.getNotifyOnLastFailure());
    executingData.put("flowOverride"+executionId,executionOptions.getFlowParameters());
    executingData.put("cycle_date"+executionId,getCycleDate(flow));
    executingData.put("failureEmailsOverride"+executionId,executionOptions.isFailureEmailsOverridden());
    executingData.put("successEmailsOverride"+executionId,executionOptions.isSuccessEmailsOverridden());
    executingData.put("projectId"+executionId,flow.getProjectId());
    executingData.put("project"+executionId,flow.getProjectName());
    executingData.put("ajax"+executionId,"executeFlow");
    executingData.put("flow"+executionId,flow.getFlowId());
    executingData.put("executionId"+executionId,flow.getExecutionId());

    executingData.put("concurrentOption"+executionId,executionOptions.getConcurrentOption());
    if( "queue".equals(executionOptions.getConcurrentOption())){
      executingData.put("queueLevel"+executionId,executionOptions.getQueueLevel());
    }
    if("pipeline".equals(executionOptions.getConcurrentOption())){
      executingData.put("pipelineLevel"+executionId,executionOptions.getPipelineLevel());
    }
    executingData.put("disabledList"+executionId,executionOptions.getDisabledJobs());
    return executingData;
  }
}
