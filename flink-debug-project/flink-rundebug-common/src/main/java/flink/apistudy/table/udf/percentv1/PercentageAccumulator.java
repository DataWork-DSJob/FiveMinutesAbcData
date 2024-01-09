package flink.apistudy.table.udf.percentv1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @projectName: FiveMinutesAbcData
 * @className: PercentageAccumulator
 * @description: flink.apistudy.table.udf.percentv1.PercentageAccumulator
 * @author: jiaqing.he
 * @date: 2023/8/29 13:41
 * @version: 1.0
 */

//!只针对时间来计算95线等,其它参数不要使用本类
public class PercentageAccumulator {

    private static final Logger LOG            = LoggerFactory
            .getLogger(PercentageAccumulator.class);

    public final static double  PERCENT_50     = 0.5;

    public final static double  PERCENT_75     = 0.25;

    public final static double  PERCENT_90     = 0.1;

    public final static double  PERCENT_95     = 0.05;

    public final static double  PERCENT_99     = 0.01;

    public final static double  PERCENT_999    = 0.001;

    public final static double  PERCENT_9999   = 0.0001;

    public final static int     PERCENT_COUNT  = 7;

    private final static int[]  SCALE          = {             //
            1,          //
            2,          //
            4,          //
            8,          //
            16,         //
            32,         //
            64,         //
            128,        //
            256,        //
            512,        //
            1024,       //
            2048,       //
            4096,       //
            8192,       //
            16384,      //
            32768,      //
            65536       //
    };

    private int[]               countContainer = {             //
            0,          //<=1
            0,          //<=2
            0,          //<=4
            0,          //<=8
            0,          //<=16
            0,          //<=32
            0,          //<=64
            0,          //<=128
            0,          //<=256
            0,          //<=512
            0,          //<=1024
            0,          //<=2048
            0,          //<=4096
            0,          //<=8192
            0,          //<=16384
            0,          //<=32768
            0           //<=65536
    };

    private int positionByTwoDivision(int[] array, int begin, int end, int value) {
        int mid = (begin + end) >> 1;
        int midValue = array[mid];
        int halfMidValue = midValue >> 1;
        //判断是否可以命中mid
        if (value > halfMidValue && value <= midValue) {
            return mid;
        }
        //没法命中,则根据大小来定
        if (value <= halfMidValue) {
            if (mid - 1 < 0) {//没路可走的边界条件
                return 0;
            }
            return positionByTwoDivision(array, begin, mid - 1, value);
        } else {
            return positionByTwoDivision(array, mid + 1, end, value);
        }

    }

    public int positionInValueArray(int val) {
        int length = SCALE.length;
        //如果大于最大值|小于等于最小值
        if (val >= SCALE[length - 1]) {
            return length - 1;
        } else if (val <= SCALE[0]) {
            return 0;
        }
        //采用2分法来计算
        return positionByTwoDivision(SCALE, 0, length - 1, val);
    }

    public void accumulate(Object value) {
        //转换为long值,int值够用了
        Long longValue = (Long) value;
        int intValue = longValue.intValue();
        //找到下标
        int index = positionInValueArray(intValue);
        countContainer[index]++;
    }

    //确保在[1,MAX]范围内,
    //自然顺序
    private int adjust(int input, int max) {
        if (input <= 1) {
            return 1;
        } else if (input >= max) {
            return max;
        } else {
            return input;
        }
    }

    private static final ThreadLocal<StringBuilder> STR_BUILDER_ThreadLocal = new ThreadLocal<StringBuilder>() {
        public StringBuilder initialValue() {
            return new StringBuilder();
        }
    };
    private static final String                     SEPARATOR               = ":";

    public String getValue() {

        //total
        int total = 0;
        int length = countContainer.length;
        for (int index = 0; index < length; index++) {
            total += countContainer[index];
        }

        //如果total为0的异常情况
        //注意是自然序---[1,total]
        int percent_9999_pos = adjust((int) (total * PERCENT_9999), total);
        boolean found_9999 = false;
        int percent_9999_value = Integer.MAX_VALUE;
        //999
        int percent_999_pos = adjust((int) (total * PERCENT_999), total);
        boolean found_999 = false;
        int percent_999_value = Integer.MAX_VALUE;
        //99
        int percent_99_pos = adjust((int) (total * PERCENT_99), total);
        boolean found_99 = false;
        int percent_99_value = Integer.MAX_VALUE;
        //95
        int percent_95_pos = adjust((int) (total * PERCENT_95), total);
        boolean found_95 = false;
        int percent_95_value = Integer.MAX_VALUE;
        //90
        int percent_90_pos = adjust((int) (total * PERCENT_90), total);
        boolean found_90 = false;
        int percent_90_value = Integer.MAX_VALUE;
        //75
        int percent_75_pos = adjust((int) (total * PERCENT_75), total);
        boolean found_75 = false;
        int percent_75_value = Integer.MAX_VALUE;
        //50
        int percent_50_pos = adjust((int) (total * PERCENT_50), total);
        boolean found_50 = false;
        int percent_50_value = Integer.MAX_VALUE;

        //开始遍历每一个元素,从后往前算
        int scanned = 0;
        int left = PERCENT_COUNT;
        for (int index = length - 1; index >= 0; index--) {
            //当前没有值,无论如何也不会成为备选
            if (0 == countContainer[index]) {
                continue;
            }
            //当前有值
            scanned += countContainer[index];
            //逐个判断
            //9999线
            if (false == found_9999 && scanned >= percent_9999_pos) {
                percent_9999_value = SCALE[index];
                found_9999 = true;
                left--;
            }
            //999线
            if (false == found_999 && scanned >= percent_999_pos) {
                percent_999_value = SCALE[index];
                found_999 = true;
                left--;
            }
            //99线
            if (false == found_99 && scanned >= percent_99_pos) {
                percent_99_value = SCALE[index];
                found_99 = true;
                left--;
            }
            //95线
            if (false == found_95 && scanned >= percent_95_pos) {
                percent_95_value = SCALE[index];
                found_95 = true;
                left--;
            }
            //90线
            if (false == found_90 && scanned >= percent_90_pos) {
                percent_90_value = SCALE[index];
                found_90 = true;
                left--;
            }
            //75线
            if (false == found_75 && scanned >= percent_75_pos) {
                percent_75_value = SCALE[index];
                found_75 = true;
                left--;
            }
            //50线
            if (false == found_50 && scanned >= percent_50_pos) {
                percent_50_value = SCALE[index];
                found_50 = true;
                left--;
            }
            //全部都找到了就break
            if (0 == left) {
                break;
            }
        }

        //所有的值都算好了
        //拿出来时先reset一下
        StringBuilder stringBuilder = STR_BUILDER_ThreadLocal.get();
        stringBuilder.delete(0, stringBuilder.length());
        //开始挂各种数据,测试表明每秒几百万次执行
        stringBuilder.append(percent_50_value);
        stringBuilder.append(SEPARATOR);
        stringBuilder.append(percent_75_value);
        stringBuilder.append(SEPARATOR);
        stringBuilder.append(percent_90_value);
        stringBuilder.append(SEPARATOR);
        stringBuilder.append(percent_95_value);
        stringBuilder.append(SEPARATOR);
        stringBuilder.append(percent_99_value);
        stringBuilder.append(SEPARATOR);
        stringBuilder.append(percent_999_value);
        stringBuilder.append(SEPARATOR);
        stringBuilder.append(percent_9999_value);
        //return
        return stringBuilder.toString();
    }

    public void print() {
        for (int index = 0; index < this.countContainer.length; index++) {
            System.out.println(index + "->" + this.countContainer[index]);
        }
    }
}