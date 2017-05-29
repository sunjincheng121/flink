package org.apache.flink.table.api.java.stream.utils;

/**
 * Created by jincheng.sunjc on 17/5/27.
 */
public class TPojo implements java.io.Serializable {
    private Long id;
    private String name;

    public TPojo(){
        this(1L,"ss");
    }

    public TPojo(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Pojo{" +
               "id=" + id +
               ", name='" + name + '\'' +
               '}';
    }
}