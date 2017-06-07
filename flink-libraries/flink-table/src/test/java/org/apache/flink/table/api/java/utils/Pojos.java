package org.apache.flink.table.api.java.utils;

import java.io.Serializable;

public class Pojos {
    public static class AtomicTypePojo implements Serializable{
        public Long getScore() {
            return score;
        }

        public void setScore(Long score) {
            this.score = score;
        }

        private Long score;
    }

    public static class User implements Serializable{

        private String name;
        private int age;

        public Long getScore() {
            return score;
        }

        public void setScore(Long score) {
            this.score = score;
        }

        private Long score;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
