package org.apache.flink.table.api.java.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.sources.DefinedRowtimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;

public class SongEventTableSource implements StreamTableSource<Row>, DefinedRowtimeAttribute {

    private static final TypeInformation<SongEvent> typeInfo = TypeInformation.of(SongEvent.class);

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

class MySourceFunction implements SourceFunction<SongEvent> {
    private ArrayList<SongEvent> datas;

    public MySourceFunction(ArrayList<SongEvent> datas) {
        this.datas = datas;
    }

    @Override
    public void run(SourceContext<SongEvent> ctx) throws Exception {
        for (SongEvent songEvent : datas) {

            if (songEvent.getType() == SongEventType.PLAY) {
                ctx.emitWatermark(new Watermark(songEvent.getTimestamp()));
            }
            ctx.collectWithTimestamp(songEvent, songEvent.getTimestamp());
        }
    }

    @Override
    public void cancel() {

    }
}

class Song implements Serializable {
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String name;
    private Long length;
    private String author;

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }
}

class SongEvent implements Serializable{
    private Song song;
    private Long userId;
    private Long timestamp;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Song getSong() {
        return song;
    }

    public void setSong(Song song) {
        this.song = song;
    }

    public Long getUserId() {
        return userId;
    }

    public SongEventType getType() {
        return type;
    }

    public void setType(SongEventType type) {
        this.type = type;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    private SongEventType type;
}

enum SongEventType {
    PLAY, OTHER;
}