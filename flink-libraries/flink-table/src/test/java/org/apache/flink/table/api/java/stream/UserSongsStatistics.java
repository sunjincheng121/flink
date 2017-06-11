package org.apache.flink.table.api.java.stream;

import java.io.Serializable;
import java.sql.Timestamp;

public class UserSongsStatistics implements Serializable {
    private String songName;

    private Long userId;

    private Long cnt;
    private Timestamp winStart;

    public Timestamp getWinStart() {
        return winStart;
    }

    public void setWinStart(Timestamp $f0) {
        this.winStart = $f0;
    }

    public Timestamp getWinEnd() {
        return winEnd;
    }

    public void setWinEnd(Timestamp $f1) {
        this.winEnd = $f1;
    }

    private Timestamp winEnd;

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
               ", w$start=" + winStart +
               ", w$end=" + winEnd +
               '}';
    }
}
