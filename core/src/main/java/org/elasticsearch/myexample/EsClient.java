package org.elasticsearch.myexample;

import java.net.InetAddress;
import java.net.UnknownHostException;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

public class EsClient {
    /**
     * 日志类
     */
//    protected final Logger logger = LogManager.getLogger(EsClient.class.getName());

    /**
     * 集群节点信息集合
     */
    private EsEnv esEnv;

    /**
     * es客户端
     */
    private Client client;

    /**
     * 构造器
     * Creates a new instance of EsClient.
     *
     * @param esEnv
     */
    public EsClient(EsEnv esEnv) {
        this.esEnv = esEnv;
        buildClient();
    }

    /**
     * 
     * getClient:获取客户端. <br/>
     *
     * @author qiyongkang
     * @return
     * @since JDK 1.6
     */
    public Client getClient() {
        return client;
    }

    /**
     * 
     * addTransport:添一个ip到集群. <br/>
     *
     * @author qiyongkang
     * @param host
     * @param port
     * @return
     * @since JDK 1.6
     */
    protected EsClient addTransport(String host, int port) {
        try {
            ((TransportClient) client)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
//            logger.error("添加ip地址异常", e);
        }
        return this;
    }

    /**
     * 
     * buildClient:生成一个客户端. <br/>
     *
     * @author qiyongkang
     * @since JDK 1.6
     */
    private void buildClient() {
        //设置
        Settings settings = Settings.settingsBuilder().put("cluster.name", esEnv.getClusterName())
                .put("transport.tcp.compress", true).build();

        //ip数组
        String[] arrIp = esEnv.getIp().split(",");
        //端口数组
        String[] arrPort = esEnv.getPort().split(",");

        TransportAddress[] addressArr = new TransportAddress[arrPort.length];
        for (int i = 0, size = arrIp.length; i < size; i++) {
            String ip = arrIp[i];
            int port = 9300;
            try {
                port = Integer.valueOf(arrPort[i]);
            } catch (NumberFormatException e) {
//                logger.error("port trans error !");
            }
            try {
                addressArr[i] = new InetSocketTransportAddress(InetAddress.getByName(ip), port);
            } catch (UnknownHostException e) {
                e.printStackTrace();
//                logger.error("添加ip地址异常", e);
            }
        }

        client = TransportClient.builder().settings(settings).build().addTransportAddresses(addressArr);
//        logger.info("开辟集群连接,address:{}，连接对象：{}", addressArr, client);

    }

    /**
     * 
     * rebuildClient:重新生成客户端. <br/>
     *
     * @author qiyongkang
     * @since JDK 1.6
     */
    public void rebuildClient() {
//        logger.info("上次client连接发生错误,重新开辟连接!");
        if (client != null) {
            close();
        }
        buildClient();
    }

    /**
     * 
     * getBulkRequestBuilder:获取批量操作的builder. <br/>
     *
     * @author qiyongkang
     * @return
     * @since JDK 1.6
     */
    public BulkRequestBuilder getBulkRequestBuilder() {
        return client.prepareBulk();
    }

    /**
     * 
     * getIndexRequestBuilder:获取索引请求的builder. <br/>
     *
     * @author qiyongkang
     * @return
     * @since JDK 1.6
     */
    public IndexRequestBuilder getIndexRequestBuilder() {
        return client.prepareIndex();
    }

    /**
     * 
     * getUpdateRequestBuilder:获取索引更新请求的builder. <br/>
     *
     * @author qiyongkang
     * @return
     * @since JDK 1.6
     */
    public UpdateRequestBuilder getUpdateRequestBuilder() {
        return client.prepareUpdate();
    }

    /**
     * 
     * getEsSearch:获取搜索请求的builder. <br/>
     *
     * @author qiyongkang
     * @return
     * @since JDK 1.6
     */
    public SearchRequestBuilder getSearchRequestBuilder() {
        return client.prepareSearch();
    }

    /**
     * 
     * esRefresh:刷新. <br/>
     *
     * @author qiyongkang
     * @since JDK 1.6
     */
    public void esRefresh() {
        client.admin().indices().prepareRefresh().execute().actionGet();
    }

    /**
     * 获取ES服务器的所有打开的索引
     * 
     * @Description:
     * @return
     */
    public ClusterHealthResponse getClusterHealthResponse() {
        return client.admin().cluster().health(Requests.clusterHealthRequest().waitForGreenStatus()).actionGet();
    }

    /**
     * 获取ES服务器的所有索引（包括打开和关闭的索引）
     * 
     * @Description:
     * @return
     */
    public MetaData getMetaData() {
        ClusterState state = client.admin().cluster().prepareState().execute().actionGet().getState();
        return state.getMetaData();
    }

    /**
     * 
     * isExists:判断索引名是否存在. <br/>
     *
     * @author qiyongkang
     * @param indexName
     * @return
     * @since JDK 1.6
     */
    public boolean isExists(String indexName) {
        return client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
    }

    /**
     * 
     * closeIndex:关闭索引. <br/>
     *
     * @author qiyongkang
     * @param indexName
     * @return
     * @since JDK 1.6
     */
    public boolean closeIndex(String indexName) {
        return client.admin().indices().prepareClose(indexName).execute().actionGet().isAcknowledged();
    }

    /**
     * 
     * closeIndex:打开索引. <br/>
     *
     * @author qiyongkang
     * @param indexName
     * @return
     * @since JDK 1.6
     */
    public boolean openIndex(String indexName) {
        return client.admin().indices().prepareOpen(indexName).execute().actionGet().isAcknowledged();
    }

    /**
     * 
     * deleteIndex:删除索引. <br/>
     *
     * @author qiyongkang
     * @param indexName
     * @return
     * @since JDK 1.6
     */
    public boolean deleteIndex(String indexName) {
        return client.admin().indices().prepareDelete(indexName).execute().actionGet().isAcknowledged();
    }

    /**
     * 
     * createIndex:创建一个索引. <br/>
     *
     * @author qiyongkang
     * @param settings
     * @param indexName
     * @return
     * @since JDK 1.6
     */
    public boolean createIndex(Settings settings, String indexName) {
        return client.admin().indices().prepareCreate(indexName).setSettings(settings).execute().actionGet()
                .isAcknowledged();
    }

    /**
     * 
     * createAlias:创建索引的别名. <br/>
     *
     * @author qiyongkang
     * @param indexName
     * @param aliasName
     * @since JDK 1.6
     */
    public void createAlias(String indexName, String aliasName) {
        client.admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
    }

    /**
     * 
     * createAlias:删除索引的别名. <br/>
     *
     * @author qiyongkang
     * @param indexName
     * @param aliasName
     * @since JDK 1.6
     */
    public void deleteAlias(String indexName, String aliasName) {
        client.admin().indices().prepareAliases().removeAlias(indexName, aliasName).execute().actionGet();
    }

    /**
     * 
     * putMapping:设置类型的字段映射. <br/>
     *
     * @author qiyongkang
     * @param mappingRequest
     * @return
     * @since JDK 1.6
     */
    public boolean putMapping(PutMappingRequest mappingRequest) {
        return client.admin().indices().putMapping(mappingRequest).actionGet().isAcknowledged();
    }

    /**
     * 
     * optimize:优化索引. <br/>
     *
     * @author qiyongkang
     * @param indexName
     * @return
     * @since JDK 1.6
     */
    public OptimizeResponse optimize(String indexName) {
        OptimizeRequest optRequest = new OptimizeRequest(new String[] { indexName });
        // optRequest.flush(true);
        optRequest.maxNumSegments(1);
        OptimizeResponse optResponse = client.admin().indices().optimize(optRequest).actionGet();
        return optResponse;
    }

    /**
     * 关闭ES客户端
     * 
     * @Description:
     */
    public void close() {
        if (client != null) {
//            logger.info("关闭连接对象成功:{}", client);
            client.close();
        }
    }

}
