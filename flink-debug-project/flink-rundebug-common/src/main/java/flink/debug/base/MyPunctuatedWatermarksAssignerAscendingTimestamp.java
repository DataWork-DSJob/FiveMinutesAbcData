package flink.debug.base;

import flink.debug.utils.RowUtils;
import flink.debug.utils.TimeHelper;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyPunctuatedWatermarksAssignerAscendingTimestamp
 * @description: flink.debug.base.MyPunctuatedWatermarksAssignerAscendingTimestamp
 * @author: jiaqing.he
 * @date: 2023/4/11 10:15
 * @version: 1.0
 */
public class MyPunctuatedWatermarksAssignerAscendingTimestamp implements AssignerWithPunctuatedWatermarks<Row> {
    /**
     *  需要依赖于事件本身的某些属性决定是否发射水印的情况。我们实现checkAndGetNextWatermark()方法来产生水印，产生的时机完全由用户控制。
     *
     */

    private final Logger LOG = LoggerFactory.getLogger(MyPunctuatedWatermarksAssignerAscendingTimestamp.class);
    private long lastEmittedWatermark = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {
        Long timestamp = RowUtils.getOrDefault(element, 2, 0L);
        if (timestamp < lastEmittedWatermark) {
            String lastWm = TimeHelper.Format.formatAsHHmmssSSS(lastEmittedWatermark);
            String eventTime = TimeHelper.Format.formatAsHHmmssSSS(timestamp);
            String previousTime = TimeHelper.Format.formatAsHHmmssSSS(previousElementTimestamp);
            long deltaMs = lastEmittedWatermark - timestamp;
            LOG.info("LateData延迟数据 ( {} < 当前水位{}, 延迟{} )  event:{}, previous:{}, wm:{} ; CurElement={}", timestamp, lastEmittedWatermark, deltaMs,
                    eventTime, previousTime, lastWm, element);
        }
        return timestamp;
    }
    @Override
    public Watermark checkAndGetNextWatermark(Row lastElement, long extractedTimestamp) {
        Long lastEventTime = RowUtils.getOrDefault(lastElement, 2, 0L);

        // 时间戳没增加 100毫秒, 更新一次水位;
        final long previousWm = this.lastEmittedWatermark;
        boolean needUpdateWm = extractedTimestamp > previousWm + 100;
        if (needUpdateWm) {
            long deltaMs = extractedTimestamp - previousWm;
            String lastWm = TimeHelper.Format.formatAsHHmmssSSS(previousWm);
            String newWm = TimeHelper.Format.formatAsHHmmssSSS(extractedTimestamp);
            LOG.debug("更新水位( {} -> {} 递增{} ms) 时间从{} 涨到{}; lastElement数据 {}", previousWm, extractedTimestamp, deltaMs, lastWm, newWm, lastElement);
            this.lastEmittedWatermark = extractedTimestamp;
        }
        return needUpdateWm ? new Watermark(extractedTimestamp) : null;
    }
}
