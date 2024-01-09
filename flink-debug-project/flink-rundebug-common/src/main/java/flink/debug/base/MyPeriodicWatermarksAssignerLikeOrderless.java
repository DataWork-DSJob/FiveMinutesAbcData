package flink.debug.base;

import flink.debug.utils.RowUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyPeriodicWatermarksAssignerLikeOrderless
 * @description: flink.debug.base.MyPeriodicWatermarksAssignerLikeOrderless
 * @author: jiaqing.he
 * @date: 2023/4/11 10:15
 * @version: 1.0
 */
public class MyPeriodicWatermarksAssignerLikeOrderless implements AssignerWithPeriodicWatermarks<Row> {
    /** 周期性产生的水位,
     *  generate watermarks in a periodical interval. At most every i milliseconds (configured via ExecutionConfig.getAutoWatermarkInterval()), the system will call the getCurrentWatermark() method to probe for the next watermark value
     *   周期频率: 200ms,
     *      periodic-watermarks-interval, ExecutionConfig.setAutoWatermarkInterval(), 默认 200L 毫秒;
     *      能过于频繁。因为Watermark对象是会全部流向下游的，也会实打实地占用内存，水印过多会造成系统性能下降。
     *
     */
    private static final long serialVersionUID = 1L;
    /** The current maximum timestamp seen so far. */
    private long currentMaxTimestamp;
    /** The timestamp of the last emitted watermark. */
    private long lastEmittedWatermark = Long.MIN_VALUE;
    /**
     * The (fixed) interval between the maximum seen timestamp seen in the records
     * and that of the watermark to be emitted.
     */
    private final long maxOutOfOrderness;

    public MyPeriodicWatermarksAssignerLikeOrderless(Time maxOutOfOrderness) {
        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the maximum allowed " + "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        }
        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        // this guarantees that the watermark never goes backwards.
        long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        return new Watermark(lastEmittedWatermark);
    }

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        Long timestamp = RowUtils.getOrDefault(element, 2, System.currentTimeMillis());
        if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp;
        }
        return timestamp;
    }
}