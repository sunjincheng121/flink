package org.apache.flink.table.api.java.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.utils.*;
public class NewStreamSql {
    public static void main(String[] args) throws Exception {
//        withoutWindowProp();
//        withWindowPropF0();
//        withWindowPropUsingUDF();
        withWindowProp();

    }

    public static void withWindowProp() throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(sEnv);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv.registerTableSource("songs", new NewSongEventTableSource());
        tEnv.registerFunction("toLong", new Func100());
        final Table table = tEnv.sql(
                "SELECT " +
                "TUMBLE_START(t, INTERVAL '3' SECOND) as winStart , " +
                "TUMBLE_END(t, INTERVAL '3' SECOND) as winEnd, " +
                "COUNT(1) as cnt, " +
                "song_name as songName, " +
                "userId " +
                "FROM songs " +
                "WHERE type = 'PLAY' " +
                "GROUP BY song_name, userId, TUMBLE(t, INTERVAL '3' SECOND)");

        DataStream<UserSongsStatistics> ds =
                tEnv.toAppendStream(table, TypeInformation.of(UserSongsStatistics.class));

        ds.print();

        sEnv.execute();
    }

    public static void withWindowPropUsingUDF() throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(sEnv);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv.registerTableSource("songs", new NewSongEventTableSource());
        tEnv.registerFunction("toLong", new Func100());
        final Table table = tEnv.sql(
                "SELECT " +
                "toLong(TUMBLE_START(t, INTERVAL '3' SECOND)) as winStart , " +
                "toLong(TUMBLE_END(t, INTERVAL '3' SECOND)) as winEnd, " +
                "COUNT(1) as cnt, " +
                "song_name as songName, " +
                "userId " +
                "FROM songs " +
                "WHERE type = 'PLAY' " +
                "GROUP BY song_name, userId, TUMBLE(t, INTERVAL '3' SECOND)");

        DataStream<UserSongsStatistics> ds =
                tEnv.toAppendStream(table, TypeInformation.of(UserSongsStatistics.class));

        ds.print();

        sEnv.execute();
    }

    public static void withWindowPropF0() throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(sEnv);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv.registerTableSource("songs", new NewSongEventTableSource());
        tEnv.registerFunction("toLong", new Func100());
        final Table table = tEnv.sql(
                "SELECT " +
                "toLong(TUMBLE_START(t, INTERVAL '3' SECOND)) as $f0 , " +
                "toLong(TUMBLE_END(t, INTERVAL '3' SECOND)) as $f1, " +
                "COUNT(1) as cnt, " +
                "song_name as songName, " +
                "userId " +
                "FROM songs " +
                "WHERE type = 'PLAY' " +
                "GROUP BY song_name, userId, TUMBLE(t, INTERVAL '3' SECOND)");

        DataStream<UserSongsStatisticsF0> ds =
                tEnv.toAppendStream(table, TypeInformation.of(UserSongsStatisticsF0.class));

        ds.print();

        sEnv.execute();
    }

    public static void withoutWindowProp() throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(sEnv);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tEnv.registerTableSource("songs", new NewSongEventTableSource());

        final Table table = tEnv.sql(
                "SELECT " +
                "COUNT(1) as cnt, " +
                "song_name as songName, " +
                "userId " +
                "FROM songs " +
                "WHERE type = 'PLAY' " +
                "GROUP BY song_name, userId, TUMBLE(t, INTERVAL '3' SECOND)");

        DataStream<UserSongsWithoutStatistics> ds =
                tEnv.toAppendStream(table, TypeInformation.of(UserSongsWithoutStatistics.class));

        ds.print();

        sEnv.execute();
    }
}
