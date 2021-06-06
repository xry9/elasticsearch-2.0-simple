package myexample;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

/**
 * ClassName:AbstractEntityService <br/>
 * Date:     2015年11月27日 上午9:49:33 <br/>
 * @author   qiyongkang
 * @version  
 * @since    JDK 1.6
 * @see      
 */
public abstract class AbstractEntityService<T> {

    protected EsClient esclient = null;

    /**
     * 索引名
     */
    protected String indexName;

    /**
     * 类型名
     */
    protected String typeName;

    /**
     * 日志类
     */
//    protected static Logger logger = LogManager.getLogger(AbstractEntityService.class);

    /**
     * 
     * 构造器
     *
     * @param client
     */
    public AbstractEntityService(EsClient client, String indexName, String typeName) {
        this.esclient = client;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    /**
     * 
     * getEsIdByPrimary:根据唯一键查出_id. <br/>
     *
     * @author qiyongkang
     * @param dict
     * @return
     * @since JDK 1.6
     */
    public String getEsIdByPrimary(KeyValue dict) {
        String esId = null;
        SearchRequestBuilder srb = this.esclient.getSearchRequestBuilder().setIndices(this.indexName).setTypes(this.typeName);

        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        //id
        if (dict.getValue() != null) {
            bool.must(QueryBuilders.termQuery(dict.getKey(), dict.getValue()));
        }

        SearchResponse searchResponse = srb.setPostFilter(bool)
        .addSort(dict.getKey(), SortOrder.ASC)
        .execute().actionGet();

        SearchHit[] arrHits = searchResponse.getHits().getHits();
        if (arrHits != null && arrHits.length > 0) {
            esId = arrHits[0].getId();
        }

//        logger.info("唯一键：{}的_id为{}", dict, esId);
        return esId;
    }

    /**
     * 
     * insertBulkSize:分批插入. <br/>
     *
     * @author qiyongkang
     * @param bulkRequest
     * @since JDK 1.6
     */
    @SuppressWarnings("rawtypes")
    protected void insertBulkSize(BulkRequestBuilder bulkRequest) {
        List<ActionRequest> actionRequestList = null;
        //更新
        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
//            logger.error("更新失败:{}", response.buildFailureMessage());
        } else {
//            logger.info("批量更新成功，数目为{}", bulkRequest.request().requests().size());
        }
        //清空
        actionRequestList = bulkRequest.request().requests();
        actionRequestList.clear();
    }
}
