import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class firstTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> left = env.socketTextStream("localhost", 9000);
        left.print("left-->>");
        SingleOutputStreamOperator<String> leftAndTime = left.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.split(",")[1]);
                    }
                }
        );


        DataStreamSource<String> right = env.socketTextStream("localhost", 8000);
        right.print("right-->>");
        SingleOutputStreamOperator<String> rightAndTime = right.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String s) {
                        return Long.parseLong(s.split(",")[1]);
                    }
                }
        );


        SingleOutputStreamOperator<String> intervalJoinDS = leftAndTime
                .keyBy(a -> a.split(",")[0])
                .intervalJoin(rightAndTime.keyBy(b -> b.split(",")[0]))
                .between(Time.milliseconds(-10), Time.milliseconds(1))
                .process(new ProcessJoinFunction<String, String, String>() {
                    @Override
                    public void processElement(String l, String r, Context ctx, Collector<String> out) throws Exception {
                        out.collect(l + "+++++" + r);
                    }
                });


        intervalJoinDS.print("join-->>>>     ");


        env.execute();
    }
}
