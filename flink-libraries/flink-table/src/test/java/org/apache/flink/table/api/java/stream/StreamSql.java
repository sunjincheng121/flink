package org.apache.flink.table.api.java.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class StreamSql {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(sEnv);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv.registerTableSource("songs", new SongEventTableSource());

        final Table table = tEnv.sql(
                "SELECT " +
                "TUMBLE_START(t, INTERVAL '3' SECOND) as wStart, " +
                "TUMBLE_END(t, INTERVAL '3' SECOND) as wEnd, " +
                "COUNT(1) as cnt, " +
                "song_name as songName, " +
                "userId " +
                "FROM songs " +
                "WHERE type = 'PLAY' " +
                "GROUP BY song_name, userId, TUMBLE(t, INTERVAL '3' SECOND)");

        tEnv.toAppendStream(table, TypeInformation.of(Row.class)).print();

        sEnv.execute();
    }
}
