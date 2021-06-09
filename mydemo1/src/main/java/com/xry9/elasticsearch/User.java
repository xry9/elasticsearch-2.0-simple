package com.xry9.elasticsearch;

import java.util.Date;

/**
 * ClassName:User <br/>
 * Date: 2015年11月24日 下午5:12:09 <br/>
 * 
 * @author qiyongkang
 * @version
 * @since JDK 1.6
 * @see
 */
public class User {
    /**
     * es自动生成的id
     */
    private String esId;

    /**
     * id
     */
    private Long id;

    /**
     * 姓名
     */
    private String name;

    /**
     * 年龄
     */
    private Integer age;

    /**
     * 性别,0->男，1->女
     */
    private Integer sex;

    /**
     * 生日,pattern:'yyyy-MM-dd HH:mm:ss'
     */
    private Date birthday;

    /**
     * 起始时间,pattern:'yyyy-MM-dd HH:mm:ss'
     */
    private Date startTime;

    /**
     * 结束时间,pattern:'yyyy-MM-dd HH:mm:ss'
     */
    private Date endTime;

    /**
     * 起始年龄
     */
    private Integer startAge;

    /**
     * 结束年龄
     */
    private Integer endAge;

    public User() {
    }

    public User(Long id, String name, Integer age, Integer sex, Date birthday) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.birthday = birthday;
    }

    public String getEsId() {
        return esId;
    }

    public void setEsId(String esId) {
        this.esId = esId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Integer getStartAge() {
        return startAge;
    }

    public void setStartAge(Integer startAge) {
        this.startAge = startAge;
    }

    public Integer getEndAge() {
        return endAge;
    }

    public void setEndAge(Integer endAge) {
        this.endAge = endAge;
    }

    @Override
    public String toString() {
        return "User [esId=" + esId + ",id=" + id + ", name=" + name + ", age=" + age + ", sex=" + sex + ", birthday=" + birthday + "]";
    }
}
