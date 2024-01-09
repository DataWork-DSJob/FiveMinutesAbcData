package flink.debug.pressure;

import flink.debug.entity.TradeEvent;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TradeEventSource
 * @description: flink.debug.pressure.TradeEventSource
 * @author: jiaqing.he
 * @date: 2023/3/18 18:51
 * @version: 1.0
 */
public class TradeEventSource extends SimpleSource<TradeEvent>{
    public TradeEventSource(int maxBatch, TradeEvent... data) {
        super(maxBatch, data);
    }
}
