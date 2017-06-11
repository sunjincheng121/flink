package org.apache.flink.table.api.java.stream;

import java.io.Serializable;

public class UserSongsStatisticsF0 implements Serializable {
    private String songName;

    private Long userId;

    private Long cnt;
    private Long $f0;

    public Long get$f0() {
        return $f0;
    }

    public void set$f0(Long $f0) {
        this.$f0 = $f0;
    }

    public Long get$f1() {
        return $f1;
    }

    public void set$f1(Long $f1) {
        this.$f1 = $f1;
    }

    private Long $f1;

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
               ", w$start=" + $f0 +
               ", w$end=" + $f1 +
               '}';
    }
}
