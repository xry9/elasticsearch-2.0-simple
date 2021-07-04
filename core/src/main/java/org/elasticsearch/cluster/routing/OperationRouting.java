/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import java.util.*;

/**
 *
 */
public class OperationRouting extends AbstractComponent {



    private final AwarenessAllocationDecider awarenessAllocationDecider;

    @Inject
    public OperationRouting(Settings settings, AwarenessAllocationDecider awarenessAllocationDecider) {
        super(settings);
        this.awarenessAllocationDecider = awarenessAllocationDecider;
    }

    public ShardIterator indexShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) {
        return shards(clusterState, index, type, id, routing).shardsIt();
    }

    public ShardIterator deleteShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing) {
        return shards(clusterState, index, type, id, routing).shardsIt();
    }

    public ShardIterator getShards(ClusterState clusterState, String index, String type, String id, @Nullable String routing, @Nullable String preference) {
        return preferenceActiveShardIterator(shards(clusterState, index, type, id, routing), clusterState.nodes().localNodeId(), clusterState.nodes(), preference);
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        return preferenceActiveShardIterator(shards(clusterState, index, shardId), clusterState.nodes().localNodeId(), clusterState.nodes(), preference);
    }

    public GroupShardsIterator broadcastDeleteShards(ClusterState clusterState, String index) {
        return indexRoutingTable(clusterState, index).groupByShardsIt();
    }

    public int searchShardsCount(ClusterState clusterState, String[] concreteIndices, @Nullable Map<String, Set<String>> routing) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        return shards.size();
    }
    public GroupShardsIterator searchShards(ClusterState clusterState, String[] concreteIndices, @Nullable Map<String, Set<String>> routing, @Nullable String preference) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = new HashSet<>(shards.size());
        //xlogger.info("===searchShards===83===" + shards.size() + "===" + Arrays.toString(concreteIndices) + "===" + routing);
        for (IndexShardRoutingTable shard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(shard, clusterState.nodes().localNodeId(), clusterState.nodes(), preference);
            if (iterator != null) {
                set.add(iterator);
            }
        }
        return new GroupShardsIterator(Lists.newArrayList(set));
    }
    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();
    private Set<IndexShardRoutingTable> computeTargetedShards(ClusterState clusterState, String[] concreteIndices, @Nullable Map<String, Set<String>> routing) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    int shardId = shardId(clusterState, index, null, null, r);
                    IndexShardRoutingTable indexShard = indexRouting.shard(shardId);
                    if (indexShard == null) {
                        throw new ShardNotFoundException(new ShardId(index, shardId));
                    }
                    //xlogger.info("===computeTargetedShards===110==="+index+"==="+indexShard);
                    // we might get duplicates, but that's ok, they will override one another
                    set.add(indexShard);
                }
            } else {
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    //xlogger.info("===computeTargetedShards===116==="+index+"==="+indexShard.getShardId());
                    set.add(indexShard);
                }
            }
        }
        return set;
    }
    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard, String localNodeId, DiscoveryNodes nodes, @Nullable String preference) {
        //xlogger.info("===preferenceActiveShardIterator===124==="+(preference == null || preference.isEmpty()));
        if (preference == null || preference.isEmpty()) {
            String[] awarenessAttributes = awarenessAllocationDecider.awarenessAttributes();
            //xlogger.info("===preferenceActiveShardIterator===127==="+(awarenessAttributes.length == 0));
            if (awarenessAttributes.length == 0) {
                return indexShard.activeInitializingShardsRandomIt();
            } else {
                return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
            }
        }
        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf(';');
                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    String[] awarenessAttributes = awarenessAllocationDecider.awarenessAttributes();
                    if (awarenessAttributes.length == 0) {
                        return indexShard.activeInitializingShardsRandomIt();
                    } else {
                        return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
                    }
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);
                }
            }
            preferenceType = Preference.parse(preference);
            //xlogger.info("===preferenceActiveShardIterator===170==="+preferenceType.type());
            switch (preferenceType) {
                case PREFER_NODE:
                    return indexShard.preferNodeActiveInitializingShardsIt(preference.substring(Preference.PREFER_NODE.type().length() + 1));
                case LOCAL:
                    return indexShard.preferNodeActiveInitializingShardsIt(localNodeId);
                case PRIMARY:
                    return indexShard.primaryActiveInitializingShardIt();
                case REPLICA:
                    return indexShard.replicaActiveInitializingShardIt();
                case PRIMARY_FIRST:
                    return indexShard.primaryFirstActiveInitializingShardsIt();
                case REPLICA_FIRST:
                    return indexShard.replicaFirstActiveInitializingShardsIt();
                case ONLY_LOCAL:
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODE:
                    String nodeId = preference.substring(Preference.ONLY_NODE.type().length() + 1);
                    ensureNodeIdExists(nodes, nodeId);
                    return indexShard.onlyNodeActiveInitializingShardsIt(nodeId);
                case ONLY_NODES:
                    String nodeAttribute = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttribute, nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index
        String[] awarenessAttributes = awarenessAllocationDecider.awarenessAttributes();
        //xlogger.info("===preferenceActiveShardIterator===199==="+(awarenessAttributes.length == 0));
        if (awarenessAttributes.length == 0) {
            return indexShard.activeInitializingShardsIt(DjbHashFunction.DJB_HASH(preference));
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes, DjbHashFunction.DJB_HASH(preference));
        }
    }
    public IndexMetaData indexMetaData(ClusterState clusterState, String index) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        if (indexMetaData == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetaData;
    }
    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }


    // either routing is set, or type/id are set
    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String type, String id, String routing) {
        int shardId = shardId(clusterState, index, type, id, routing);
        logger.info("===shards===222==="+id+"==="+shardId);
        return shards(clusterState, index, shardId);
    }
    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, int shardId) {
        IndexShardRoutingTable indexShard = indexRoutingTable(clusterState, index).shard(shardId);
        if (indexShard == null) {
            throw new ShardNotFoundException(new ShardId(index, shardId));
        }
        logger.info("===shards===230==="+shardId+"==="+indexShard.shardId);
        return indexShard;
    }

    @SuppressForbidden(reason = "Math#abs is trappy")
    private int shardId(ClusterState clusterState, String index, String type, String id, @Nullable String routing) {
        final IndexMetaData indexMetaData = indexMetaData(clusterState, index);
        final Version createdVersion = indexMetaData.getCreationVersion();
        final HashFunction hashFunction = indexMetaData.getRoutingHashFunction();
        final boolean useType = indexMetaData.getRoutingUseType();
        logger.info("===shardId===240==="+type+"==="+id);try { Integer.parseInt("shardId"); }catch (Exception e){logger.error("===", e);}
        final int hash;
        if (routing == null) {
            if (!useType) {
                hash = hash(hashFunction, id);
            } else {
                hash = hash(hashFunction, type, id);
            }
        } else {
            hash = hash(hashFunction, routing);
        }
        if (createdVersion.onOrAfter(Version.V_2_0_0_beta1)) {
            return MathUtils.mod(hash, indexMetaData.numberOfShards());
        } else {
            return Math.abs(hash % indexMetaData.numberOfShards());
        }
    }

    protected int hash(HashFunction hashFunction, String routing) {
        return hashFunction.hash(routing);
    }

    @Deprecated
    protected int hash(HashFunction hashFunction, String type, String id) {
        if (type == null || "_all".equals(type)) {
            throw new IllegalArgumentException("Can't route an operation with no type and having type part of the routing (for backward comp)");
        }
        return hashFunction.hash(type, id);
    }

    private void ensureNodeIdExists(DiscoveryNodes nodes, String nodeId) {
        if (!nodes.dataNodes().keys().contains(nodeId)) {
            throw new IllegalArgumentException("No data node with id[" + nodeId + "] found");
        }
    }

}
