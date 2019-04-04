import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 实例连接：
 * https://ci.apache.org/projects/flink/flink-docs-release-1.7/tutorials/local_setup.html
 *
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
@SuppressWarnings("serial")
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
//        final String hostname;

        //定义连接端口
        final int port = 9999;
//        try {
//            final ParameterTool params = ParameterTool.fromArgs(args);
//            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
//            port = params.getInt("port");
//        } catch (Exception e) {
//            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
//                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
//                    "and port is the address of the text server");
//            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
//                    "type the input text into the command line");
//            return;
//        }

        //得到执行环境对象
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接到socket后读取输入的data
        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");

        //解析数据，分组，然后窗口化
        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text

                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        //对输入的数据，按照空白符(制表符，空格，回车都算)进行切分
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })
                //将对象WordWithCount的word字段对应数据的key
                .keyBy("word")
                //设置窗口的时间长度
                .timeWindow(Time.seconds(5))
                //按照key进行聚合
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /**
     * Data type for words with count.
     */
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}