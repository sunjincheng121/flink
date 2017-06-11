package org.apache.flink.table.api.java.stream;

import java.io.Serializable;
import java.sql.Timestamp;

public class UserSongsWithoutStatistics implements Serializable {
    private String songName;

    private Long userId;

    private Long cnt;

    private Timestamp w$start;

    private Timestamp w$end;

    public Timestamp getW$start() {
        return w$start;
    }

    public void setW$start(Timestamp w$start) {
        this.w$start = w$start;
    }

    public Timestamp getW$end() {
        return w$end;
    }

    public void setW$end(Timestamp w$end) {
        this.w$end = w$end;
    }

    public String getSongName() {
        return songName;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setSongName(String name) {
        this.songName = name;
    }


    public Long getCnt() {
        return cnt;
    }

    public void setCnt(Long cnt) {
        this.cnt = cnt;
    }

    @Override
    public String toString() {
        return "UserSongsStatistics{" +
               "songName='" + songName + '\'' +
               ", userId=" + userId +
               ", cnt=" + cnt +
               '}';
    }
}
