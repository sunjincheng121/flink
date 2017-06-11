package org.apache.flink.table.api.java.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.sources.DefinedRowtimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;

public class NewSongEventTableSource implements StreamTableSource<Row>, DefinedRowtimeAttribute {

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {

        Song s = new Song();
        s.setName("hi");
        s.setAuthor("hh");
        s.setLength(10L);
        SongEvent e = new SongEvent();
        e.setSong(s);
        e.setTimestamp(200303290342L);
        e.setType(SongEventType.PLAY);
        e.setUserId(1L);

        Song s2 = new Song();
        s2.setName("hi=====================>");
        s2.setAuthor("hh===================>");
        s2.setLength(20L);
        SongEvent e2 = new SongEvent();
        e2.setSong(s2);
        e2.setTimestamp(200303L);
        e2.setType(SongEventType.PLAY);
        e2.setUserId(2L);

        ArrayList<SongEvent> datas = new ArrayList<SongEvent>();
        datas.add(e);
        datas.add(e2);


        return env.addSource(new MySourceFunction(datas)).map(new MapFunction<SongEvent, Row>() {
            @Override
            public Row map(SongEvent songEvent) throws Exception {
                return Row.of(
                        songEvent.getSong().getName(),
                        songEvent.getSong().getLength(),
                        songEvent.getSong().getAuthor(),
                        songEvent.getUserId(),
                        songEvent.getType().toString());
            }
        }).returns(getReturnType());
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW_NAMED(
                new String[]{"song_name", "song_length", "song_author", "userId", "type"},
                Types.STRING, Types.LONG, Types.STRING, Types.LONG, Types.STRING
        );
    }
    @Override
    public String explainSource() {
        return "SongEventTable";
    }

    @Override
    public String getRowtimeAttribute() {
        return "t";
    }
}


