package flink.debug.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TradeEvent
 * @description: flink.debug.entity.TradeEvent
 * @author: jiaqing.he
 * @date: 2023/3/17 16:40
 * @version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeEvent implements Serializable {
    private static final String DEFAULT_TRADE_ID = "TID_";
    private static final String DEFAULT_BRANCH_ID = "BID_";
    private static final String DEFAULT_OK_CODE = "0000";
    private static final String DEFAULT_FAILED_CODE = "0002";

    private String tradeId;
    private String tradeTime;
    private String branchId;
    private int tradeType;
    private double tradeAmount;
    private String resultCode;

    public static TradeEvent create(int suffixNum, String tradeTime) {
        if (null == tradeTime || tradeTime.isEmpty()) {
            tradeTime = LocalDateTime.now().toString();
        }
        float random = (float) Math.random();
        TradeEvent tradeEvent = new TradeEvent(
                DEFAULT_TRADE_ID + suffixNum,
                tradeTime,
                DEFAULT_BRANCH_ID + suffixNum,
                1,
                100 + random,
                DEFAULT_OK_CODE);
        return tradeEvent;
    }



}
