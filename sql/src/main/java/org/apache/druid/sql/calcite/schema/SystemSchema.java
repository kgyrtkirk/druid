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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.DefaultEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SystemSchema extends AbstractSchema
{
  private static final String SEGMENTS_TABLE = "segments";
  private static final String SERVERS_TABLE = "servers";
  private static final String SERVER_SEGMENTS_TABLE = "server_segments";
  private static final String TASKS_TABLE = "tasks";
  private static final String SUPERVISOR_TABLE = "supervisors";

  private static final Function<SegmentStatusInCluster, Iterable<ResourceAction>>
      SEGMENT_STATUS_IN_CLUSTER_RA_GENERATOR = segment ->
      Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(
          segment.getDataSegment().getDataSource())
      );

  private static final Function<DataSegment, Iterable<ResourceAction>> SEGMENT_RA_GENERATOR =
      segment -> Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(
          segment.getDataSource())
      );

  private static final long REPLICATION_FACTOR_UNKNOWN = -1L;

  /**
   * Booleans constants represented as long type,
   * where 1 = true and 0 = false to make it easy to count number of segments
   * which are published, available etc.
   */
  private static final long IS_ACTIVE_FALSE = 0L;
  private static final long IS_ACTIVE_TRUE = 1L;
  private static final long IS_PUBLISHED_FALSE = 0L;
  private static final long IS_PUBLISHED_TRUE = 1L;
  private static final long IS_AVAILABLE_TRUE = 1L;
  private static final long IS_OVERSHADOWED_FALSE = 0L;
  private static final long IS_OVERSHADOWED_TRUE = 1L;

  static final RowSignature SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("segment_id", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("start", ColumnType.STRING)
      .add("end", ColumnType.STRING)
      .add("size", ColumnType.LONG)
      .add("version", ColumnType.STRING)
      .add("partition_num", ColumnType.LONG)
      .add("num_replicas", ColumnType.LONG)
      .add("num_rows", ColumnType.LONG)
      .add("is_active", ColumnType.LONG)
      .add("is_published", ColumnType.LONG)
      .add("is_available", ColumnType.LONG)
      .add("is_realtime", ColumnType.LONG)
      .add("is_overshadowed", ColumnType.LONG)
      .add("shard_spec", ColumnType.STRING)
      .add("dimensions", ColumnType.STRING)
      .add("metrics", ColumnType.STRING)
      .add("projections", ColumnType.STRING)
      .add("last_compaction_state", ColumnType.STRING)
      .add("replication_factor", ColumnType.LONG)
      .build();

  /**
   * List of [0..n) where n is the size of {@link #SEGMENTS_SIGNATURE}.
   */
  private static final int[] SEGMENTS_PROJECT_ALL = IntStream.range(0, SEGMENTS_SIGNATURE.size()).toArray();

  /**
   * Fields in {@link #SEGMENTS_SIGNATURE} that are serialized with {@link ObjectMapper#writeValueAsString(Object)}.
   */
  private static final IntSet SEGMENTS_JSON_FIELDS = new IntOpenHashSet(
      new int[]{
          SEGMENTS_SIGNATURE.indexOf("shard_spec"),
          SEGMENTS_SIGNATURE.indexOf("dimensions"),
          SEGMENTS_SIGNATURE.indexOf("metrics"),
          SEGMENTS_SIGNATURE.indexOf("projections"),
          SEGMENTS_SIGNATURE.indexOf("last_compaction_state")
      }
  );

  static final RowSignature SERVERS_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("host", ColumnType.STRING)
      .add("plaintext_port", ColumnType.LONG)
      .add("tls_port", ColumnType.LONG)
      .add("server_type", ColumnType.STRING)
      .add("tier", ColumnType.STRING)
      .add("curr_size", ColumnType.LONG)
      .add("max_size", ColumnType.LONG)
      .add("is_leader", ColumnType.LONG)
      .add("start_time", ColumnType.STRING)
      .build();

  static final RowSignature SERVER_SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("server", ColumnType.STRING)
      .add("segment_id", ColumnType.STRING)
      .build();

  static final RowSignature TASKS_SIGNATURE = RowSignature
      .builder()
      .add("task_id", ColumnType.STRING)
      .add("group_id", ColumnType.STRING)
      .add("type", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("created_time", ColumnType.STRING)
      .add("queue_insertion_time", ColumnType.STRING)
      .add("status", ColumnType.STRING)
      .add("runner_status", ColumnType.STRING)
      .add("duration", ColumnType.LONG)
      .add("location", ColumnType.STRING)
      .add("host", ColumnType.STRING)
      .add("plaintext_port", ColumnType.LONG)
      .add("tls_port", ColumnType.LONG)
      .add("error_msg", ColumnType.STRING)
      .build();

  static final RowSignature SUPERVISOR_SIGNATURE = RowSignature
      .builder()
      .add("supervisor_id", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("state", ColumnType.STRING)
      .add("detailed_state", ColumnType.STRING)
      .add("healthy", ColumnType.LONG)
      .add("type", ColumnType.STRING)
      .add("source", ColumnType.STRING)
      .add("suspended", ColumnType.LONG)
      .add("spec", ColumnType.STRING)
      .build();

  private final Map<String, Table> tableMap;

  @Inject
  public SystemSchema(
      final DruidSchema druidSchema,
      final MetadataSegmentView metadataView,
      final TimelineServerView serverView,
      final FilteredServerInventoryView serverInventoryView,
      final AuthorizerMapper authorizerMapper,
      final CoordinatorClient coordinatorClient,
      final OverlordClient overlordClient,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(serverView, "serverView");
    this.tableMap = ImmutableMap.of(
        SEGMENTS_TABLE,
        new SegmentsTable(druidSchema, metadataView, jsonMapper, authorizerMapper),
        SERVERS_TABLE,
        new ServersTable(
            druidNodeDiscoveryProvider,
            serverInventoryView,
            authorizerMapper,
            overlordClient,
            coordinatorClient
        ),
        SERVER_SEGMENTS_TABLE,
        new ServerSegmentsTable(serverView, authorizerMapper),
        TASKS_TABLE,
        new TasksTable(overlordClient, authorizerMapper),
        SUPERVISOR_TABLE,
        new SupervisorsTable(overlordClient, authorizerMapper)
    );
  }

  @Override
  public Map<String, Table> getTableMap()
  {
    return tableMap;
  }

  /**
   * This table contains row per segment from metadata store as well as served segments.
   */
  static class SegmentsTable extends AbstractTable implements ProjectableFilterableTable
  {
    private final DruidSchema druidSchema;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;
    private final MetadataSegmentView metadataView;

    public SegmentsTable(
        DruidSchema druidSchemna,
        MetadataSegmentView metadataView,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper
    )
    {
      this.druidSchema = druidSchemna;
      this.metadataView = metadataView;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SEGMENTS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(
        final DataContext root,
        final List<RexNode> filters,
        @Nullable final int[] projects
    )
    {
      // get available segments from druidSchema
      final BrokerSegmentMetadataCache availableMetadataCache = druidSchema.cache();

      // Keep track of which segments we emitted from the publishedSegments iterator, so we don't emit them again
      // from the availableSegments iterator.
      final Set<SegmentId> segmentsAlreadySeen =
          Sets.newHashSetWithExpectedSize(availableMetadataCache.getTotalSegments());

      // Get segments from metadata segment cache (if enabled in SQL planner config), else directly from
      // Coordinator. This may include both published and realtime segments.
      final Iterator<SegmentStatusInCluster> metadataStoreSegments = metadataView.getSegments();
      final FluentIterable<Object[]> publishedSegments = FluentIterable
          .from(() -> getAuthorizedPublishedSegments(metadataStoreSegments, root))
          .transform(val -> {
            final DataSegment segment = val.getDataSegment();
            final AvailableSegmentMetadata availableSegmentMetadata =
                availableMetadataCache.getAvailableSegmentMetadata(segment.getDataSource(), segment.getId());
            segmentsAlreadySeen.add(segment.getId());
            long numReplicas = 0L, numRows = 0L, isRealtime, isAvailable = 0L;

            if (availableSegmentMetadata != null) {
              numReplicas = availableSegmentMetadata.getNumReplicas();
              isAvailable = availableSegmentMetadata.getNumReplicas() > 0 ? IS_AVAILABLE_TRUE : IS_ACTIVE_FALSE;
              numRows = availableSegmentMetadata.getNumRows();
            }

            // If druid.centralizedDatasourceSchema.enabled is set on the Coordinator, SegmentMetadataCache on the
            // broker might have outdated or no information regarding numRows and rowSignature for a segment.
            // In that case, we should use {@code numRows} from the segment polled from the coordinator.
            if (null != val.getNumRows()) {
              numRows = val.getNumRows();
            }

            isRealtime = val.isRealtime() ? 1 : 0;

            // set of segments returned from Coordinator include published and realtime segments
            // so realtime segments are not published and vice versa
            boolean isPublished = !val.isRealtime();

            // is_active is true for published segments that are not overshadowed or else they should be realtime
            boolean isActive = isPublished ? !val.isOvershadowed() : val.isRealtime();

            return new Object[]{
                segment.getId(),
                segment.getDataSource(),
                segment.getInterval().getStart(),
                segment.getInterval().getEnd(),
                segment.getSize(),
                segment.getVersion(),
                (long) segment.getShardSpec().getPartitionNum(),
                numReplicas,
                numRows,
                isActive ? IS_ACTIVE_TRUE : IS_ACTIVE_FALSE,
                isPublished ? IS_PUBLISHED_TRUE : IS_PUBLISHED_FALSE,
                isAvailable,
                isRealtime,
                val.isOvershadowed() ? IS_OVERSHADOWED_TRUE : IS_OVERSHADOWED_FALSE,
                segment.getShardSpec(),
                segment.getDimensions(),
                segment.getMetrics(),
                segment.getProjections(),
                segment.getLastCompactionState(),
                // If the segment is unpublished, we won't have this information yet.
                // If the value is null, the load rules might have not evaluated yet, and we don't know the replication factor.
                // This should be automatically updated in the next Coordinator poll.
                val.getReplicationFactor() == null ? REPLICATION_FACTOR_UNKNOWN : (long) val.getReplicationFactor()
            };
          });

      // If druid.centralizedDatasourceSchema.enabled is set on the Coordinator, all the segments in this loop
      // would be covered in the previous iteration since Coordinator would return realtime segments as well.
      final FluentIterable<Object[]> availableSegments = FluentIterable
          .from(() -> getAuthorizedAvailableSegments(availableMetadataCache.iterateSegmentMetadata(), root))
          .transform(val -> {
            final DataSegment segment = val.getSegment();
            if (segmentsAlreadySeen.contains(segment.getId())) {
              return null;
            }
            return new Object[]{
                segment.getId(),
                segment.getDataSource(),
                segment.getInterval().getStart(),
                segment.getInterval().getEnd(),
                segment.getSize(),
                segment.getVersion(),
                (long) segment.getShardSpec().getPartitionNum(),
                val.getNumReplicas(),
                val.getNumRows(),
                // is_active is true for unpublished segments iff they are realtime
                val.isRealtime() /* is_active */,
                // is_published is false for unpublished segments
                IS_PUBLISHED_FALSE,
                // is_available is assumed to be always true for segments announced by historicals or realtime tasks
                IS_AVAILABLE_TRUE,
                val.isRealtime(),
                IS_OVERSHADOWED_FALSE,
                // there is an assumption here that unpublished segments are never overshadowed
                segment.getShardSpec(),
                segment.getDimensions(),
                segment.getMetrics(),
                segment.getProjections(),
                null, // unpublished segments from realtime tasks will not be compacted yet
                REPLICATION_FACTOR_UNKNOWN // If the segment is unpublished, we won't have this information yet.
            };
          });

      final Iterable<Object[]> allSegments = Iterables.unmodifiableIterable(
          Iterables.concat(publishedSegments, availableSegments)
      );

      return Linq4j.asEnumerable(allSegments)
                   .where(Objects::nonNull)
                   .select(row -> projectSegmentsRow(row, projects, jsonMapper));
    }

    private Iterator<SegmentStatusInCluster> getAuthorizedPublishedSegments(
        Iterator<SegmentStatusInCluster> it,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      final Iterable<SegmentStatusInCluster> authorizedSegments = AuthorizationUtils
          .filterAuthorizedResources(
              authenticationResult,
              () -> it,
              SEGMENT_STATUS_IN_CLUSTER_RA_GENERATOR,
              authorizerMapper
          );
      return authorizedSegments.iterator();
    }

    private Iterator<AvailableSegmentMetadata> getAuthorizedAvailableSegments(
        Iterator<AvailableSegmentMetadata> availableSegmentEntries,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      Function<AvailableSegmentMetadata, Iterable<ResourceAction>> raGenerator = segment ->
          Collections.singletonList(
              AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(segment.getSegment().getDataSource())
          );

      final Iterable<AvailableSegmentMetadata> authorizedSegments =
          AuthorizationUtils.filterAuthorizedResources(
              authenticationResult,
              () -> availableSegmentEntries,
              raGenerator,
              authorizerMapper
          );

      return authorizedSegments.iterator();
    }

    private static class PartialSegmentData
    {
      private final long isAvailable;
      private final long isRealtime;
      private final long numReplicas;
      private final long numRows;

      public PartialSegmentData(
          final long isAvailable,
          final long isRealtime,
          final long numReplicas,
          final long numRows
      )

      {
        this.isAvailable = isAvailable;
        this.isRealtime = isRealtime;
        this.numReplicas = numReplicas;
        this.numRows = numRows;
      }

      public long isAvailable()
      {
        return isAvailable;
      }

      public long isRealtime()
      {
        return isRealtime;
      }

      public long getNumReplicas()
      {
        return numReplicas;
      }

      public long getNumRows()
      {
        return numRows;
      }
    }
  }

  /**
   * This table contains row per server. It contains all the discovered servers in Druid cluster.
   * Some columns like tier and size are only applicable to historical nodes which contain segments.
   */
  static class ServersTable extends AbstractTable implements ScannableTable
  {
    // This is used for maxSize and currentSize when they are unknown.
    // The unknown size doesn't have to be 0, it's better to be null.
    // However, this table is returning 0 for them for some reason and we keep the behavior for backwards compatibility.
    // Maybe we can remove this and return nulls instead when we remove the bindable query path which is currently
    // used to query system tables.
    private static final long UNKNOWN_SIZE = 0L;

    private final AuthorizerMapper authorizerMapper;
    private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
    private final FilteredServerInventoryView serverInventoryView;
    private final OverlordClient overlordClient;
    private final CoordinatorClient coordinatorClient;

    public ServersTable(
        DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
        FilteredServerInventoryView serverInventoryView,
        AuthorizerMapper authorizerMapper,
        OverlordClient overlordClient,
        CoordinatorClient coordinatorClient
    )
    {
      this.authorizerMapper = authorizerMapper;
      this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
      this.serverInventoryView = serverInventoryView;
      this.overlordClient = overlordClient;
      this.coordinatorClient = coordinatorClient;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SERVERS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final Iterator<DiscoveryDruidNode> druidServers = getDruidServers(druidNodeDiscoveryProvider);
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );
      checkStateReadAccessForServers(authenticationResult, authorizerMapper);

      String tmpCoordinatorLeader = "";
      String tmpOverlordLeader = "";

      try {
        tmpCoordinatorLeader = FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true).toString();
      }
      catch (Exception ignored) {
        // no reason to kill the results if something is sad and there are no leaders
      }

      try {
        tmpOverlordLeader = FutureUtils.getUnchecked(overlordClient.findCurrentLeader(), true).toString();
      }
      catch (Exception ignored) {
        // no reason to kill the results if something is sad and there are no leaders
      }

      final String coordinatorLeader = tmpCoordinatorLeader;
      final String overlordLeader = tmpOverlordLeader;

      final FluentIterable<Object[]> results = FluentIterable
          .from(() -> druidServers)
          .transform((DiscoveryDruidNode discoveryDruidNode) -> {
            //noinspection ConstantConditions
            final boolean isDiscoverableDataServer = isDiscoverableDataServer(
                discoveryDruidNode.getService(DataNodeService.DISCOVERY_SERVICE_KEY, DataNodeService.class)
            );
            final NodeRole serverRole = discoveryDruidNode.getNodeRole();

            if (isDiscoverableDataServer) {
              final DruidServer druidServer = serverInventoryView.getInventoryValue(
                  discoveryDruidNode.getDruidNode().getHostAndPortToUse()
              );
              if (druidServer != null || NodeRole.HISTORICAL.equals(serverRole)) {
                // Build a row for the data server if that server is in the server view, or the node type is historical.
                // The historicals are usually supposed to be found in the server view. If some historicals are
                // missing, it could mean that there are some problems in them to announce themselves. We just fill
                // their status with nulls in this case.
                return buildRowForDiscoverableDataServer(discoveryDruidNode, druidServer);
              } else {
                return buildRowForNonDataServer(discoveryDruidNode);
              }
            } else if (NodeRole.COORDINATOR.equals(serverRole)) {
              return buildRowForNonDataServerWithLeadership(
                  discoveryDruidNode,
                  coordinatorLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
              );
            } else if (NodeRole.OVERLORD.equals(serverRole)) {
              return buildRowForNonDataServerWithLeadership(
                  discoveryDruidNode,
                  overlordLeader.contains(discoveryDruidNode.getDruidNode().getHostAndPortToUse())
              );
            } else {
              return buildRowForNonDataServer(discoveryDruidNode);
            }
          });
      return Linq4j.asEnumerable(results);
    }


    /**
     * Returns a row for all node types which don't serve data. The returned row contains only static information.
     */
    private static Object[] buildRowForNonDataServer(DiscoveryDruidNode discoveryDruidNode)
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          null,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          null,
          toStringOrNull(discoveryDruidNode.getStartTime())
      };
    }

    /**
     * Returns a row for all node types which don't serve data. The returned row contains only static information.
     */
    private static Object[] buildRowForNonDataServerWithLeadership(
        DiscoveryDruidNode discoveryDruidNode,
        boolean isLeader
    )
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          null,
          UNKNOWN_SIZE,
          UNKNOWN_SIZE,
          isLeader ? 1L : 0L,
          toStringOrNull(discoveryDruidNode.getStartTime())
      };
    }

    /**
     * Returns a row for discoverable data server. This method prefers the information from
     * {@code serverFromInventoryView} if available which is the current state of the server. Otherwise, it
     * will get the information from {@code discoveryDruidNode} which has only static configurations.
     */
    private static Object[] buildRowForDiscoverableDataServer(
        DiscoveryDruidNode discoveryDruidNode,
        @Nullable DruidServer serverFromInventoryView
    )
    {
      final DruidNode node = discoveryDruidNode.getDruidNode();
      final DruidServer druidServerToUse = serverFromInventoryView == null
                                           ? toDruidServer(discoveryDruidNode)
                                           : serverFromInventoryView;
      final long currentSize;
      if (serverFromInventoryView == null) {
        // If server is missing in serverInventoryView, the currentSize should be unknown
        currentSize = UNKNOWN_SIZE;
      } else {
        currentSize = serverFromInventoryView.getCurrSize();
      }
      return new Object[]{
          node.getHostAndPortToUse(),
          node.getHost(),
          (long) node.getPlaintextPort(),
          (long) node.getTlsPort(),
          StringUtils.toLowerCase(discoveryDruidNode.getNodeRole().toString()),
          druidServerToUse.getTier(),
          currentSize,
          druidServerToUse.getMaxSize(),
          null,
          toStringOrNull(discoveryDruidNode.getStartTime())
      };
    }

    private static boolean isDiscoverableDataServer(DataNodeService dataNodeService)
    {
      return dataNodeService != null && dataNodeService.isDiscoverable();
    }

    private static DruidServer toDruidServer(DiscoveryDruidNode discoveryDruidNode)
    {
      final DruidNode druidNode = discoveryDruidNode.getDruidNode();
      final DataNodeService dataNodeService = discoveryDruidNode.getService(
          DataNodeService.DISCOVERY_SERVICE_KEY,
          DataNodeService.class
      );
      if (isDiscoverableDataServer(dataNodeService)) {
        return new DruidServer(
            druidNode.getHostAndPortToUse(),
            druidNode.getHostAndPort(),
            druidNode.getHostAndTlsPort(),
            dataNodeService.getMaxSize(),
            dataNodeService.getServerType(),
            dataNodeService.getTier(),
            dataNodeService.getPriority()
        );
      } else {
        throw new ISE("[%s] is not a discoverable data server", discoveryDruidNode);
      }
    }

    private static Iterator<DiscoveryDruidNode> getDruidServers(DruidNodeDiscoveryProvider druidNodeDiscoveryProvider)
    {
      return Arrays.stream(NodeRole.values())
                   .flatMap(nodeRole -> druidNodeDiscoveryProvider.getForNodeRole(nodeRole).getAllNodes().stream())
                   .collect(Collectors.toList())
                   .iterator();
    }
  }

  /**
   * This table contains row per segment per server.
   */
  static class ServerSegmentsTable extends AbstractTable implements ScannableTable
  {
    private final TimelineServerView serverView;
    final AuthorizerMapper authorizerMapper;

    public ServerSegmentsTable(TimelineServerView serverView, AuthorizerMapper authorizerMapper)
    {
      this.serverView = serverView;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SERVER_SEGMENTS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );
      checkStateReadAccessForServers(authenticationResult, authorizerMapper);

      final List<Object[]> rows = new ArrayList<>();
      final List<ImmutableDruidServer> druidServers = serverView.getDruidServers();
      final int serverSegmentsTableSize = SERVER_SEGMENTS_SIGNATURE.size();
      for (ImmutableDruidServer druidServer : druidServers) {
        final Iterable<DataSegment> authorizedServerSegments = AuthorizationUtils.filterAuthorizedResources(
            authenticationResult,
            druidServer.iterateAllSegments(),
            SEGMENT_RA_GENERATOR,
            authorizerMapper
        );

        for (DataSegment segment : authorizedServerSegments) {
          Object[] row = new Object[serverSegmentsTableSize];
          row[0] = druidServer.getHost();
          row[1] = segment.getId().toString();
          rows.add(row);
        }
      }
      return Linq4j.asEnumerable(rows);
    }
  }

  /**
   * This table contains row per task.
   */
  static class TasksTable extends AbstractTable implements ScannableTable
  {
    private final OverlordClient overlordClient;
    private final AuthorizerMapper authorizerMapper;

    public TasksTable(
        OverlordClient overlordClient,
        AuthorizerMapper authorizerMapper
    )
    {
      this.overlordClient = overlordClient;
      this.authorizerMapper = authorizerMapper;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(TASKS_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      class TasksEnumerable extends DefaultEnumerable<Object[]>
      {
        private final CloseableIterator<TaskStatusPlus> it;

        public TasksEnumerable(CloseableIterator<TaskStatusPlus> tasks)
        {
          this.it = getAuthorizedTasks(tasks, root);
        }

        @Override
        public Iterator<Object[]> iterator()
        {
          throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
        }

        @Override
        public Enumerator<Object[]> enumerator()
        {
          return new Enumerator<>()
          {
            @Override
            public Object[] current()
            {
              final TaskStatusPlus task = it.next();

              return new Object[]{
                  task.getId(),
                  task.getGroupId(),
                  task.getType(),
                  task.getDataSource(),
                  toStringOrNull(task.getCreatedTime()),
                  toStringOrNull(task.getQueueInsertionTime()),
                  toStringOrNull(task.getStatusCode()),
                  toStringOrNull(task.getRunnerStatusCode()),
                  task.getDuration() == null ? 0L : task.getDuration(),
                  task.getLocation().getLocation(),
                  task.getLocation().getHost(),
                  (long) task.getLocation().getPort(),
                  (long) task.getLocation().getTlsPort(),
                  task.getErrorMsg()
              };
            }

            @Override
            public boolean moveNext()
            {
              return it.hasNext();
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {
              try {
                it.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }

      return new TasksEnumerable(FutureUtils.getUnchecked(overlordClient.taskStatuses(null, null, null), true));
    }

    private CloseableIterator<TaskStatusPlus> getAuthorizedTasks(
        CloseableIterator<TaskStatusPlus> it,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      Function<TaskStatusPlus, Iterable<ResourceAction>> raGenerator = task -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(task.getDataSource()));

      final Iterable<TaskStatusPlus> authorizedTasks = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return wrap(authorizedTasks.iterator(), it);
    }

  }

  /**
   * This table contains a row per supervisor task.
   */
  static class SupervisorsTable extends AbstractTable implements ScannableTable
  {
    private final OverlordClient overlordClient;
    private final AuthorizerMapper authorizerMapper;

    public SupervisorsTable(
        OverlordClient overlordClient,
        AuthorizerMapper authorizerMapper
    )
    {
      this.overlordClient = overlordClient;
      this.authorizerMapper = authorizerMapper;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
      return RowSignatures.toRelDataType(SUPERVISOR_SIGNATURE, typeFactory);
    }

    @Override
    public TableType getJdbcTableType()
    {
      return TableType.SYSTEM_TABLE;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root)
    {
      class SupervisorsEnumerable extends DefaultEnumerable<Object[]>
      {
        private final CloseableIterator<SupervisorStatus> it;

        public SupervisorsEnumerable(CloseableIterator<SupervisorStatus> tasks)
        {
          this.it = getAuthorizedSupervisors(tasks, root);
        }

        @Override
        public Iterator<Object[]> iterator()
        {
          throw new UnsupportedOperationException("Do not use iterator(), it cannot be closed.");
        }

        @Override
        public Enumerator<Object[]> enumerator()
        {
          return new Enumerator<>()
          {
            @Override
            public Object[] current()
            {
              final SupervisorStatus supervisor = it.next();
              return new Object[]{
                  supervisor.getId(),
                  supervisor.getDataSource(),
                  supervisor.getState(),
                  supervisor.getDetailedState(),
                  supervisor.isHealthy() ? 1L : 0L,
                  supervisor.getType(),
                  supervisor.getSource(),
                  supervisor.isSuspended() ? 1L : 0L,
                  supervisor.getSpecString()
              };
            }

            @Override
            public boolean moveNext()
            {
              return it.hasNext();
            }

            @Override
            public void reset()
            {

            }

            @Override
            public void close()
            {
              try {
                it.close();
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        }
      }

      return new SupervisorsEnumerable(FutureUtils.getUnchecked(overlordClient.supervisorStatuses(), true));
    }

    private CloseableIterator<SupervisorStatus> getAuthorizedSupervisors(
        CloseableIterator<SupervisorStatus> it,
        DataContext root
    )
    {
      final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
          root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
          "authenticationResult in dataContext"
      );

      Function<SupervisorStatus, Iterable<ResourceAction>> raGenerator = supervisor -> Collections.singletonList(
          AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(supervisor.getSource()));

      final Iterable<SupervisorStatus> authorizedSupervisors = AuthorizationUtils.filterAuthorizedResources(
          authenticationResult,
          () -> it,
          raGenerator,
          authorizerMapper
      );

      return wrap(authorizedSupervisors.iterator(), it);
    }
  }

  private static <T> CloseableIterator<T> wrap(Iterator<T> iterator, Closeable closer)
  {
    return new CloseableIterator<>()
    {
      @Override
      public boolean hasNext()
      {
        final boolean hasNext = iterator.hasNext();
        if (!hasNext) {
          try {
            closer.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return hasNext;
      }

      @Override
      public T next()
      {
        return iterator.next();
      }

      @Override
      public void close() throws IOException
      {
        closer.close();
      }
    };
  }

  @Nullable
  private static String toStringOrNull(@Nullable final Object object)
  {
    if (object == null) {
      return null;
    }

    return object.toString();
  }

  /**
   * Checks if an authenticated user has the STATE READ permissions needed to view server information.
   */
  private static void checkStateReadAccessForServers(
      AuthenticationResult authenticationResult,
      AuthorizerMapper authorizerMapper
  )
  {
    final AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );

    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException("Insufficient permission to view servers: " + authResult.getErrorMessage());
    }
  }

  /**
   * Project a row using "projects" from {@link SegmentsTable#scan(DataContext, List, int[])}.
   * <p>
   * Also, fix up types so {@link ColumnType#STRING} are transformed to Strings if they aren't yet. This defers
   * computation of {@link ObjectMapper#writeValueAsString(Object)} or {@link Object#toString()} until we know we
   * actually need it.
   */
  private static Object[] projectSegmentsRow(
      final Object[] row,
      @Nullable final int[] projects,
      final ObjectMapper jsonMapper
  )
  {
    final int[] nonNullProjects = projects == null ? SEGMENTS_PROJECT_ALL : projects;
    final Object[] projectedRow = new Object[nonNullProjects.length];

    for (int i = 0; i < nonNullProjects.length; i++) {
      final Object o = row[nonNullProjects[i]];

      if (SEGMENTS_SIGNATURE.getColumnType(nonNullProjects[i]).get().is(ValueType.STRING)
          && o != null
          && !(o instanceof String)) {
        // Delay calling toString() or ObjectMapper#writeValueAsString() until we know we actually need this field.
        if (SEGMENTS_JSON_FIELDS.contains(nonNullProjects[i])) {
          try {
            projectedRow[i] = jsonMapper.writeValueAsString(o);
          }
          catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        } else {
          projectedRow[i] = o.toString();
        }
      } else {
        projectedRow[i] = o;
      }
    }
    return projectedRow;
  }
}
