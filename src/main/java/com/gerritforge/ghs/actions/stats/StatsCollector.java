// Copyright (C) 2024 GerritForge, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.ghs.actions.stats;

import com.sun.management.UnixOperatingSystemMXBean;
import java.lang.management.ManagementFactory;

public class StatsCollector {
  private final long startTime;
  private final long initialCpuTime;

  public static StatsCollector start() {
    return new StatsCollector();
  }

  private StatsCollector() {
    initialCpuTime = getProcessCpuTime();
    startTime = System.currentTimeMillis();
  }

  public StatsResult stop() {
    long executionTime = System.currentTimeMillis() - startTime;
    long consumedCpuTime = getProcessCpuTime() - initialCpuTime;

    return new StatsResult(consumedCpuTime, executionTime);
  }

  private long getProcessCpuTime() {
    return ManagementFactory.getPlatformMXBean(UnixOperatingSystemMXBean.class).getProcessCpuTime();
  }
}
