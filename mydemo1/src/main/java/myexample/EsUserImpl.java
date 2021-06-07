package myexample;

//import com.qiyongkang.es.model.User;
//import com.qiyongkang.es.service.EsUserService;
//import com.qiyongkang.es.util.DateUtil;
//import com.qiyongkang.es.util.Page;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * ClassName:EsUserImpl <br/>
 * Date: 2015年11月24日 下午5:11:52 <br/>
 *
 * @author qiyongkang
 * @version
 * @since JDK 1.6
 * @see
 */
public class EsUserImpl extends AbstractEntityService<User>implements EsUserService {
    /**
     * 日志类
     */
//    protected static Logger logger = LogManager.getLogger(EsUserImpl.class);

    /**
     *
     * 构造器
     *
     * @param client
     */
    public EsUserImpl(EsClient client) {
        super(client, "usermanage", "user");
    }

    /**
     *
     * 创建类型的字段json映射.
     *
     * @throws IOException
     */
    @Override
    public void createMappingJson() throws IOException {
        XContentBuilder xContentBuilder = null;
        xContentBuilder = jsonBuilder().startObject().startObject(typeName).startObject("_source")
                .field("enabled", true).endObject().startObject("_all").field("enabled", true).endObject()
                .startObject("properties").startObject("id").field("type", "long").field("index", "not_analyzed")
                .endObject().startObject("name").field("type", "string").endObject().startObject("age")
                .field("type", "integer").endObject().startObject("sex").field("type", "integer").endObject()
                .startObject("birthday").field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss").endObject()
                .endObject().endObject().endObject();
//        logger.info("开始创建mappingJson:{}", xContentBuilder);

        PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName).type(typeName).source(xContentBuilder);

        boolean flag = this.esclient.putMapping(mappingRequest);
//        logger.info("创建索引{}的类型{}的映射是否成功：{}", indexName, typeName, flag);
    }

    /**
     *
     * 新增json.
     *
     * @param t
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder buildEntity(User t) throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("id", t.getId())
                .field("name", t.getName()).field("age", t.getAge()).field("sex", t.getSex())
                .field("birthday", DateUtil.dateToYMDHMSStr(t.getBirthday())).endObject();
//        logger.info("build entity:{}", source);
        return source;
    }

    /**
     *
     * 将单条文档映射为对应的实体.
     *
     * @param propertiesMap
     * @return
     */
    @Override
    public User recoveEntity(Map<String, Object> propertiesMap) {
        User user = new User();
        for (String key : propertiesMap.keySet()) {
            if ("id".equals(key)) {
                user.setId(Long.valueOf(propertiesMap.get(key).toString()));
            } else if ("name".equals(key)) {
                user.setName(propertiesMap.get(key).toString());
            } else if ("age".equals(key)) {
                user.setAge(Integer.valueOf(propertiesMap.get(key).toString()));
            } else if ("sex".equals(key)) {
                user.setSex(Integer.valueOf(propertiesMap.get(key).toString()));
            } else if ("birthday".equals(key)) {
                user.setBirthday(DateUtil.strToYMDHMSDate(propertiesMap.get(key).toString()));
            }
        }
        return user;
    }

    /**
     *
     * 更新json.
     *
     * @param t
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder buildUpdateEntity(User t) throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject();
        if (t.getName() != null) {
            source.field("name", t.getName());
        }
        if (t.getAge() != null) {
            source.field("age", t.getAge());
        }
        if (t.getSex() != null) {
            source.field("sex", t.getSex());
        }
        if (t.getBirthday() != null) {
            source.field("birthday", DateUtil.dateToYMDHMSStr(t.getBirthday()));
        }
        source.endObject();
        return source;
    }

    /**
     *
     * 转换ES查找结果为实体集合
     *
     * @param searchResponse
     *            ES查找结果对象
     *            实体
     * @return t类型的实体集合
     */
    @Override
    public List<? extends Object> convertSearchResponseToList(SearchResponse searchResponse) {
        SearchHit[] arrHits = searchResponse.getHits().getHits();

        List<Object> lstEsEntity = new ArrayList<Object>(arrHits.length);
        for (int i = 0, size = arrHits.length; i < size; i++) {
            Map<String, Object> propertiesMap = (Map<String, Object>) arrHits[i].getSource();
            Object esObject = this.recoveEntity(propertiesMap);
//            logger.info("成功转换成一个对象：{}", esObject);
            lstEsEntity.add(esObject);
        }
        return lstEsEntity;
    }

    /**
     *
     * 根据id查出用户.
     *
     * @param id
     * @return
     *      String, int)
     */
    @SuppressWarnings("unchecked")
    public User getEntityById(int id) {
        User user = null;
        SearchRequestBuilder srb = this.esclient.getSearchRequestBuilder().setIndices(indexName).setTypes(typeName);

        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        // id
        bool.must(QueryBuilders.termQuery("id", id));

        SearchResponse searchResponse = srb.setPostFilter(bool).addSort("id", SortOrder.ASC).execute().actionGet();

        List<User> list = (List<User>) convertSearchResponseToList(searchResponse);
        long count = searchResponse.getHits().getTotalHits();
//        logger.info("查出的个数为{}", count);

        if (list != null && list.size() > 0) {
            user = list.get(0);
            String esId = this.getEsIdByPrimary(new KeyValue("id", user.getId().toString()));
            user.setEsId(esId);

//            logger.info("查出的用户为：{}", user);
        }
        return user;
    }

    /**
     *
     * 分页查询.
     * @param user
     * @param curretPage
     * @param pageSize
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public Page<User> getPageModel(User user, int curretPage, int pageSize) {
        Page<User> page = new Page<User>();
        SearchRequestBuilder srb = esclient.getSearchRequestBuilder().setIndices(this.indexName).setTypes(this.typeName);

        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        //id
        if (user.getId() != null) {
            bool.must(QueryBuilders.termQuery("id", user.getId()));
        }

        //name
        if (user.getName() != null) {
            bool.must(QueryBuilders.wildcardQuery("name", "*" + user.getName() + "*"));
        }

        //age
        if (user.getAge() != null) {
            bool.must(QueryBuilders.termQuery("age", user.getAge()));
        }

        //startAge
        if (user.getStartAge() != null) {
            bool.must(QueryBuilders.rangeQuery("age").from(user.getStartAge()));
        }

        //endAge
        if (user.getEndAge() != null) {
            bool.must(QueryBuilders.rangeQuery("age").to(user.getEndAge()));
        }

        //sex
        if (user.getSex() != null) {
            bool.must(QueryBuilders.termQuery("sex", user.getSex()));
        }

        //birthday
        if (user.getBirthday() != null) {
            bool.must(QueryBuilders.termQuery("birthday", DateUtil.dateToYMDHMSStr(user.getBirthday())));
        }

        //startBirth
        if (user.getStartTime() != null) {
            bool.must(QueryBuilders.rangeQuery("birthday").from(DateUtil.dateToYMDHMSStr(user.getStartTime())));
        }

        //endBirth
        if (user.getEndTime() != null) {
            bool.must(QueryBuilders.rangeQuery("birthday").to(DateUtil.dateToYMDHMSStr(user.getEndTime())));
        }

        SearchResponse searchResponse = srb.setPostFilter(bool)
        .setFrom((curretPage - 1) * pageSize)
        .setSize(pageSize)
        .addSort("id", SortOrder.ASC)
        .execute().actionGet();

//        logger.info("request:{}", srb.toString());
//        logger.info("response:{}", searchResponse.toString());

        List<User> list = (List<User>) this.convertSearchResponseToList(searchResponse);
        long count = searchResponse.getHits().getTotalHits();
//        logger.info("查出的总数为{}", count);
        page.setRows(list);
        page.setTotal(count);
        return page;
    }

    /**
     *
     * 单个插入.
     *
     * @param user
     */
    @Override
    public void singleInsert(User user) {
        boolean flag = false;
        try {
            flag = esclient.getIndexRequestBuilder().setIndex(indexName).setType(typeName)
                    .setSource(this.buildEntity(user)).execute().actionGet().isCreated();
            esclient.esRefresh();
//            logger.info("插入用户{}是否成功：{}", user, flag);
        } catch (IOException e) {
            e.printStackTrace();
//            logger.error("插入用户{}异常", user, e);
        }
    }

    /**
     *
     * 批量新增.
     *
     * @param userList
     */
    @Override
    public void batchInsert(List<User> userList) {
//        logger.info("开始批量插入，总数为{}", userList.size());

        // 每次批量插入的个数
        int bulkSize = 5000;

        BulkRequestBuilder bulkRequest = this.esclient.getBulkRequestBuilder();

        try {
            // 用于记录循环下标
            int percount = 0;
            for (int i = 0; i < userList.size(); i++) {
                User user = userList.get(i);

                XContentBuilder builder = this.buildEntity(user);
                IndexRequestBuilder indexRequestBuilder = this.esclient.getIndexRequestBuilder();

                indexRequestBuilder.setIndex(indexName);
                indexRequestBuilder.setType(typeName);
                indexRequestBuilder.setSource(builder);
                bulkRequest.add(indexRequestBuilder);

                percount++;
                if (percount >= bulkSize) {
                    insertBulkSize(bulkRequest);
                    percount = 0;
                }
            }

            if (percount > 0) {
                insertBulkSize(bulkRequest);
            }

            // 刷新
            esclient.esRefresh();
        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("批量插入过程发生异常。。", e);
        }
//        logger.info("批量插入完成");
    }

    /**
     *
     * 单个删除.
     */
    @Override
    public void singleDelete(KeyValue dict) {
        //先查出_id,然后再删除
        String _id = getEsIdByPrimary(dict);

        Client client = esclient.getClient();
        client.prepareDelete(indexName, typeName, _id).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
//        logger.info("删除_id为{}的用户{}成功", _id, dict.getValue());
    }

    /**
     *
     * 单个更新.
     * @param user
     */
    @Override
    public void singleUpdate(User user) {
//        logger.info("开始更新，更新的内容为：{}", user);

        Client client = this.esclient.getClient();
        //第一步、根据id查出_id
        String esId = this.getEsIdByPrimary(new KeyValue("id", user.getId().toString()));
        user.setEsId(esId);

        //第二步、更新
        try {
            client.prepareUpdate().setIndex(this.indexName).setType(this.typeName)
                .setId(user.getEsId()).setDoc(
                        this.buildUpdateEntity(user))
                    .execute().actionGet();
            //刷新
            this.esclient.esRefresh();
//            logger.info("更新成功：{}", user);
        } catch (IOException e) {
            e.printStackTrace();
//            logger.error("更新 发生异常", e);
        }
    }

    /**
     *
     * 批量更新.
     * @param updateList
     */
    @Override
    public void batchUpdate(List<User> updateList) {
        //第一步，查出要更新用户的_id并设置
        for (User user : updateList) {
            String esId = this.getEsIdByPrimary(new KeyValue("id", user.getId().toString()));
            user.setEsId(esId);
        }

        //第二步，批量更新
//        logger.info("开始批量更新，总数为{}", updateList.size());

        //每次批量更新的个数
        int bulkSize = 5000;

        BulkRequestBuilder bulkRequest = esclient.getBulkRequestBuilder();

        try {
            //用于记录循环下标
            int percount = 0;
            for (int i = 0; i < updateList.size(); i++) {
                User user = updateList.get(i);

                XContentBuilder builder = this.buildUpdateEntity(user);

                UpdateRequestBuilder updateRequestBuilder = esclient.getUpdateRequestBuilder();
                updateRequestBuilder.setIndex(indexName);
                updateRequestBuilder.setType(typeName);
                updateRequestBuilder.setId(user.getEsId());
                updateRequestBuilder.setDoc(builder);

                bulkRequest.add(updateRequestBuilder);

                percount++;
                if( percount >= bulkSize ){
                    insertBulkSize(bulkRequest);
                    percount = 0;
                }
            }

            if (percount > 0 ) {
                this.insertBulkSize(bulkRequest);
            }

            //刷新
            this.esclient.esRefresh();
        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("批量插入过程发生异常。。", e);
        }
//        logger.info("批量更新完成");
    }

}
