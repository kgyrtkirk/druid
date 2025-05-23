/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.k8s.overlord.common;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

public class DruidKubernetesClient implements KubernetesClientApi
{
  private final KubernetesClient kubernetesClient;

  public DruidKubernetesClient(DruidKubernetesHttpClientConfig httpClientConfig, Config kubernetesClientConfig)
  {
    this.kubernetesClient = new KubernetesClientBuilder()
        .withHttpClientFactory(new DruidKubernetesHttpClientFactory(httpClientConfig))
        .withConfig(kubernetesClientConfig)
        .build();
  }

  @Override
  public <T> T executeRequest(KubernetesExecutor<T> executor) throws KubernetesResourceNotFoundException
  {
    return executor.executeRequest(kubernetesClient);
  }

  /**
   * This client automatically gets closed by the druid lifecycle, it should not be closed when used as it is
   * meant to be reused.
   *
   * @return re-useable KubernetesClient
   */
  @Override
  public KubernetesClient getClient()
  {
    return this.kubernetesClient;
  }
}
