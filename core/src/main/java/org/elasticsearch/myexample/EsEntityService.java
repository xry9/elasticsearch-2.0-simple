package org.elasticsearch.myexample;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.qiyongkang.es.model.KeyValue;


/**
 * 
 * ES 接口实体（ES 入库实体，ES 查询返回的对象）
 * 
 *
 * @param <T>
 */
public interface EsEntityService<T extends Object> {

    /**
     * 创建T类型的ES mapping的json串
     * 
     * @param typeName
     * @return
     * @throws IOException
     */
    public void createMappingJson() throws IOException;

    /**
     * 
     * 创建T类型的ES单条记录的json串
     * 
     * @param t
     *            T类型的对象
     * @return
     * @throws IOException
     */
    public XContentBuilder buildEntity(T t) throws IOException;

    /**
     * 将ES key-value集合转换为T类型对象
     * 
     * @param propertiesMap
     *            ES 查询返回的key-value集合
     * @return
     */
    public T recoveEntity(Map<String, Object> propertiesMap);

    /**
     * 
     * 更新T类型的ES单条记录的json串
     * 
     * @param t
     *            T类型的对象
     * @return
     * @throws IOException
     */
    public XContentBuilder buildUpdateEntity(T t) throws IOException;

    /**
     * 
     * getEsIdByPrimary:根据唯一键查出_id. <br/>
     *
     * @author qiyongkang
     * @param t
     * @return
     * @since JDK 1.6
     */
    public String getEsIdByPrimary(KeyValue dict);

    /**
     * 
     * convertSearchResponseToList:将查询结果转为list. <br/>
     *
     * @author qiyongkang
     * @param searchResponse
     * @return
     * @since JDK 1.6
     */
    public List<? extends Object> convertSearchResponseToList(SearchResponse searchResponse);

}
