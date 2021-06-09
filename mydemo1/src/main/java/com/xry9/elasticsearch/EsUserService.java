package com.xry9.elasticsearch;

//import com.qiyongkang.es.util.Page;

import java.util.List;

/**
 * ClassName:EsUserService <br/>
 * Date:     2015年11月27日 上午10:20:11 <br/>
 * @author   qiyongkang
 * @version  
 * @since    JDK 1.6
 * @see      
 */
public interface  EsUserService extends EsEntityService<User> {
    /**
     * 
     * getEntityById:根据id获取实体. <br/>
     *
     * @author qiyongkang
     * @param id
     * @return
     * @since JDK 1.6
     */
    public User getEntityById(int id);

    /**
     * 
     * getPageModel:分页查询. <br/>
     *
     * @author qiyongkang
     * @param user
     * @param curretPage
     * @param pageSize
     * @return
     * @since JDK 1.6
     */
    public Page<User> getPageModel(User user, int curretPage, int pageSize);

    /**
     * 
     * singleInsert:单个新增. <br/>
     *
     * @author qiyongkang
     * @param user
     * @since JDK 1.6
     */
    public void singleInsert(User user);

    /**
     * 
     * batchInsert:批量新增. <br/>
     *
     * @author qiyongkang
     * @param userList
     * @since JDK 1.6
     */
    public void batchInsert(List<User> userList);

    /**
     * 
     * singleDelete:单个删除. <br/>
     *
     * @author qiyongkang
     * @since JDK 1.6
     */
    public void singleDelete(KeyValue dict);

    /**
     * 
     * singleUpdate:单个更新. <br/>
     *
     * @author qiyongkang
     * @param user
     * @since JDK 1.6
     */
    public void singleUpdate(User user);

    /**
     * 
     * batchUpdate:批量更新. <br/>
     *
     * @author qiyongkang
     * @param updateList
     * @since JDK 1.6
     */
    public void batchUpdate(List<User> updateList);
}
