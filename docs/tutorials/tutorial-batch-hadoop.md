---
id: tutorial-batch-hadoop
title: Load batch data using Apache Hadoop
sidebar_label: Load from Apache Hadoop
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


This tutorial shows you how to load data files into Apache Druid using a remote Hadoop cluster.

For this tutorial, we'll assume that you've already completed the previous
[batch ingestion tutorial](tutorial-batch.md) using Druid's native batch ingestion system and are using the
automatic single-machine configuration as described in the [quickstart](../operations/single-server.md).

## Install Docker

This tutorial requires [Docker](https://docs.docker.com/install/) to be installed on the tutorial machine.

Once the Docker install is complete, please proceed to the next steps in the tutorial.

## Build the Hadoop docker image

For this tutorial, we've provided a Dockerfile for a Hadoop 3.3.6 cluster, which we'll use to run the batch indexing task.

This Dockerfile and related files are located at `quickstart/tutorial/hadoop/docker`.

From the `apache-druid-{{DRUIDVERSION}}` package root, run the following commands to build a Docker image named "druid-hadoop-demo" with version tag "3.3.6":

```bash
cd quickstart/tutorial/hadoop/docker
docker build -t druid-hadoop-demo:3.3.6 .
```

This will start building the Hadoop image. Once the image build is done, you should see the message `Successfully tagged druid-hadoop-demo:3.3.6` printed to the console.

## Setup the Hadoop docker cluster

### Create temporary shared directory

We'll need a shared folder between the host and the Hadoop container for transferring some files.

Let's create some folders under `/tmp`, we will use these later when starting the Hadoop container:

```bash
mkdir -p /tmp/shared
```

### Configure /etc/hosts

On the host machine, add the following entry to `/etc/hosts`:

```
127.0.0.1 druid-hadoop-demo
```

### Start the Hadoop container

Once the `/tmp/shared` folder has been created and the `etc/hosts` entry has been added, run the following command to start the Hadoop container.

```bash
docker run -it  -h druid-hadoop-demo --name druid-hadoop-demo -p 2049:2049 -p 2122:2122 -p 8020-8042:8020-8042 -p 8088:8088 -p 8443:8443 -p 9000:9000 -p 9820:9820 -p 9860-9880:9860-9880 -p 10020:10020 -p 19888:19888 -p 34455:34455 -p 49707:49707 -p 50010:50010 -p 50020:50020 -p 50030:50030 -p 50060:50060 -p 50070:50070 -p 50075:50075 -p 50090:50090 -p 51111:51111 -v /tmp/shared:/shared druid-hadoop-demo:3.3.6 /etc/bootstrap.sh -bash
```

Once the container is started, your terminal will attach to a bash shell running inside the container:

```bash
Starting namenodes on [druid-hadoop-demo]
Starting datanodes
Starting secondary namenodes [druid-hadoop-demo]
WARNING: YARN_CONF_DIR has been replaced by HADOOP_CONF_DIR. Using value of YARN_CONF_DIR.
WARNING: YARN_OPTS has been replaced by HADOOP_OPTS. Using value of YARN_OPTS.
Starting resourcemanager
WARNING: YARN_OPTS has been replaced by HADOOP_OPTS. Using value of YARN_OPTS.
Starting nodemanagers
WARNING: YARN_OPTS has been replaced by HADOOP_OPTS. Using value of YARN_OPTS.
localhost: WARNING: YARN_OPTS has been replaced by HADOOP_OPTS. Using value of YARN_OPTS.
WARNING: YARN_CONF_DIR has been replaced by HADOOP_CONF_DIR. Using value of YARN_CONF_DIR.
WARNING: YARN_OPTS has been replaced by HADOOP_OPTS. Using value of YARN_OPTS.
WARNING: Use of this script to start the MR JobHistory daemon is deprecated.
WARNING: Attempting to execute replacement "mapred --daemon start" instead.
 * initialize hdfs for first run
bash-4.1#
```

The `Unable to load native-hadoop library for your platform... using builtin-java classes where applicable` warning messages can be safely ignored.

#### Accessing the Hadoop container shell

To open another shell to the Hadoop container, run the following command:

```
docker exec -it druid-hadoop-demo bash
```

### Test data

The startup script `bootstrap.sh`:
* creates the necessary directories
* loads an input file to HDFS
* places the hadoop configuration into the shared volume as `hadoop-conf.tgz`

## Configure Druid to use Hadoop

Some additional steps are needed to configure the Druid cluster for Hadoop batch indexing.

### Provide Hadoop configuration for Druid

From the Hadoop container's shell, run the following command to copy the Hadoop .xml configuration files to the shared folder:

```bash
cp /usr/local/hadoop/etc/hadoop/*.xml /shared/hadoop_xml
```

From the host machine, run the following, where `PATH_TO_DRUID` is replaced by the path to the Druid package.

```bash
cd $PATH_TO_DRUID
mkdir -p conf/druid/single-server/micro-quickstart/_common/hadoop-xml
tar xzf /tmp/shared/hadoop-conf.tgz -C conf/druid/single-server/micro-quickstart/_common/hadoop-xml
```

### Update Druid segment and log storage

In your favorite text editor, open `conf/druid/single-server/micro-quickstart/_common/common.runtime.properties`, and make the following edits:

#### Disable local deep storage and enable HDFS deep storage

```
#
# Deep storage
#

# For local disk (only viable in a cluster if this is a network mount):
#druid.storage.type=local
#druid.storage.storageDirectory=var/druid/segments

# For HDFS:
druid.storage.type=hdfs
druid.storage.storageDirectory=/druid/segments
```


#### Disable local log storage and enable HDFS log storage

```
#
# Indexing service logs
#

# For local disk (only viable in a cluster if this is a network mount):
#druid.indexer.logs.type=file
#druid.indexer.logs.directory=var/druid/indexing-logs

# For HDFS:
druid.indexer.logs.type=hdfs
druid.indexer.logs.directory=/druid/indexing-logs

```

### Restart Druid cluster

Once the Hadoop .xml files have been copied to the Druid cluster and the segment/log storage configuration has been updated to use HDFS, the Druid cluster needs to be restarted for the new configurations to take effect.

If the cluster is still running, CTRL-C to terminate it if running - and start it with:
```
bin/start-druid -c conf/druid/single-server/micro-quickstart
```

## Load batch data

We've included a sample of Wikipedia edits from September 12, 2015 to get you started.

To load this data into Druid, you can submit an *ingestion task* pointing to the file. We've included
a task that loads the `wikiticker-2015-09-12-sampled.json.gz` file included in the archive.

Let's submit the `wikipedia-index-hadoop3.json` task:

```bash
bin/post-index-task --file quickstart/tutorial/wikipedia-index-hadoop3.json --url http://localhost:8081
```

## Querying your data

After the data load is complete, please follow the [query tutorial](../tutorials/tutorial-query.md) to run some example queries on the newly loaded data.

## Cleanup

This tutorial is only meant to be used together with the [query tutorial](../tutorials/tutorial-query.md).

If you wish to go through any of the other tutorials, you will need to:
* Shut down the cluster and reset the cluster state by removing the contents of the `var` directory under the druid package.
* Revert the deep storage and task storage config back to local types in `conf/druid/single-server/micro-quickstart/_common/common.runtime.properties`
* Restart the cluster

This is necessary because the other ingestion tutorials will write to the same "wikipedia" datasource, and later tutorials expect the cluster to use local deep storage.

Example reverted config:

```
#
# Deep storage
#

# For local disk (only viable in a cluster if this is a network mount):
druid.storage.type=local
druid.storage.storageDirectory=var/druid/segments

# For HDFS:
#druid.storage.type=hdfs
#druid.storage.storageDirectory=/druid/segments

#
# Indexing service logs
#

# For local disk (only viable in a cluster if this is a network mount):
druid.indexer.logs.type=file
druid.indexer.logs.directory=var/druid/indexing-logs

# For HDFS:
#druid.indexer.logs.type=hdfs
#druid.indexer.logs.directory=/druid/indexing-logs

```

## Further reading

For more information on loading batch data with Hadoop, please see [the Hadoop batch ingestion documentation](../ingestion/hadoop.md).
