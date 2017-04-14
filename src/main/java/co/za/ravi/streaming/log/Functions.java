package co.za.ravi.streaming.log;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by ravikumar on 1/15/17.
 */
public class Functions {

    public static final class IpTuple implements PairFunction<ApacheAccessLog,String,Long> {

        @Override
        public Tuple2<String, Long> call(ApacheAccessLog apacheAccessLog) throws Exception {
            return new Tuple2<>(apacheAccessLog.getIpAddress(),1L);
        }
    }

    public static final class IpContentsTuple implements PairFunction<ApacheAccessLog,String,Long> {

        @Override
        public Tuple2<String, Long> call(ApacheAccessLog apacheAccessLog) throws Exception {
            return new Tuple2<>(apacheAccessLog.getIpAddress(),apacheAccessLog.getContentSize());
        }
    }

    public static final class AddLongs implements Function2<Long,Long,Long> {

        @Override
        public Long call(Long v1, Long v2) throws Exception {
            return v1+v2;
        }
    }

    public static final class SubtractLongs implements Function2<Long,Long,Long> {

        @Override
        public Long call(Long v1, Long v2) throws Exception {
            return v1-v2;
        }
    }

    public static final class LongSumReducer implements Function2<Long,Long,Long> {

        @Override
        public Long call(Long v1, Long v2) throws Exception {
            return v1+v2;
        }
    }

    public static final class ParseLogLine implements Function<String,ApacheAccessLog> {

        @Override
        public ApacheAccessLog call(String v1) throws Exception {
            return ApacheAccessLog.parseFromLogLine(v1);
        }
    }
}
