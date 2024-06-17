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

package org.apache.druid.quidem;

import com.google.inject.Inject;

import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

@Path("/quidem")
public class QuidemCaptureResource
{
  private URI quidemURI;

  @Inject
  public QuidemCaptureResource(@Named("quidem") URI quidemURI)
  {
    this.quidemURI = quidemURI;
  }

  private QuidemRecorder recorder = null;

  @GET
  @Path("/")
  @Produces(MediaType.TEXT_PLAIN)
  public String getSome()
  {
    return "Asd";
  }

  @GET
  @Path("/start")
  @Produces(MediaType.TEXT_PLAIN)
  public synchronized String getSome1() throws IOException
  {
    stopIfRunning();
    start();
    return recorder.toString();
  }

  private void start() throws IOException
  {
    recorder = new QuidemRecorder(quidemURI, new PrintStream("/tmp/new.iq"));
  }

  private void stopIfRunning()
  {
    if (recorder != null) {
      recorder.close();
      recorder = null;
    }

  }
}
