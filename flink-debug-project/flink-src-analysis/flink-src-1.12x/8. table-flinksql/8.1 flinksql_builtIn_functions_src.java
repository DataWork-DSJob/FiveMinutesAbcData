

// flink_1.12_src 


// Flink SQL 内置函数

// TO_TIMESTAMP() 函数
val STRING_TO_TIMESTAMP = Types.lookupMethod(
classOf[SqlDateTimeUtils],
"toTimestampData",
classOf[String])

// Flink SQL的内置函数, 算子的定义 FlinkSqlOperatorTable
FlinkSqlOperatorTable{

// select DATE_FORMAT('2022-03-32 13:00:32', 'yyyy-MM-dd HH:mm:ss') : String 返回字符串
public static final SqlFunction DATE_FORMAT=new SqlFunction(
		"DATE_FORMAT",
		SqlKind.OTHER_FUNCTION,
		ReturnTypes.cascade(
		ReturnTypes.explicit(SqlTypeName.VARCHAR),
		SqlTypeTransforms.FORCE_NULLABLE),
		InferTypes.RETURN_TYPE,
		OperandTypes.or(
		OperandTypes.family(SqlTypeFamily.TIMESTAMP,SqlTypeFamily.STRING),
		OperandTypes.family(SqlTypeFamily.STRING,SqlTypeFamily.STRING)
		),
		SqlFunctionCategory.TIMEDATE);

//  toTimestampData(String dateStr): TimestampData
public static final SqlFunction TO_TIMESTAMP=new SqlFunction(
		"TO_TIMESTAMP",
		SqlKind.OTHER_FUNCTION,
		ReturnTypes.cascade(
		ReturnTypes.explicit(SqlTypeName.TIMESTAMP,3),
		SqlTypeTransforms.FORCE_NULLABLE
		),
		null,
		OperandTypes.or(
		OperandTypes.family(SqlTypeFamily.CHARACTER),
		OperandTypes.family(SqlTypeFamily.CHARACTER,SqlTypeFamily.CHARACTER)),
		SqlFunctionCategory.TIMEDATE);
		*toTimestampData(String dateStr):TimestampData
		*toTimestampData(String dateStr,String format):TimestampData


// unixTimestamp(String dateStr, String format): long
public static final SqlFunction UNIX_TIMESTAMP=new SqlFunction(
		"UNIX_TIMESTAMP",
		SqlKind.OTHER_FUNCTION,
		ReturnTypes.BIGINT_NULLABLE,
		null,
		OperandTypes.or(
		OperandTypes.NILADIC,
		OperandTypes.family(SqlTypeFamily.STRING),
		OperandTypes.family(SqlTypeFamily.STRING,SqlTypeFamily.STRING)
		),
		SqlFunctionCategory.TIMEDATE);
		*unixTimestamp(String dateStr,String format):long
		*unixTimestamp(String dateStr):long
		*unixTimestamp(String dateStr,String format,TimeZone tz):long
		*


public static final SqlFunction FROM_UNIXTIME=new SqlFunction(
		"FROM_UNIXTIME",
		SqlKind.OTHER_FUNCTION,
		VARCHAR_2000_NULLABLE,
		null,
		OperandTypes.or(
		OperandTypes.family(SqlTypeFamily.INTEGER),
		OperandTypes.family(SqlTypeFamily.INTEGER,SqlTypeFamily.STRING)),
		SqlFunctionCategory.TIMEDATE);


public static final SqlFunction FROM_UNIXTIME=new SqlFunction(
		"FROM_UNIXTIME",
		SqlKind.OTHER_FUNCTION,
		VARCHAR_2000_NULLABLE,
		null,
		OperandTypes.or(
		OperandTypes.family(SqlTypeFamily.INTEGER),
		OperandTypes.family(SqlTypeFamily.INTEGER,SqlTypeFamily.STRING)
		),
		SqlFunctionCategory.TIMEDATE);
		*fromUnixtime(long unixtime):String
		*fromUnixtime(long unixtime,TimeZone tz):String


public static final SqlGroupedWindowFunction TUMBLE_START = TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_START);

public static final SqlGroupedWindowFunction TUMBLE_END = TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_END);

}


// TO_TIMESTAMP() 函数 STRING_TO_TIMESTAMP
// flink117: DateTimeUtils. parseTimestampData()
SqlDateTimeUtils.toTimestampData(String dateStr, String format): TimestampData  {
	DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(format);
	try {
		TemporalAccessor accessor = formatter.parse(dateStr);
		// complement year with 1970
		int year = accessor.isSupported(YEAR) ? accessor.get(YEAR) : 1970;
		// complement month with 1
		int month = accessor.isSupported(MONTH_OF_YEAR) ? accessor.get(MONTH_OF_YEAR) : 1;
		// complement day with 1
		int day = accessor.isSupported(DAY_OF_MONTH) ? accessor.get(DAY_OF_MONTH) : 1;
		// complement hour with 0
		int hour = accessor.isSupported(HOUR_OF_DAY) ? accessor.get(HOUR_OF_DAY) : 0;
		// complement minute with 0
		int minute = accessor.isSupported(MINUTE_OF_HOUR) ? accessor.get(MINUTE_OF_HOUR) : 0;
		// complement second with 0
		int second =accessor.isSupported(SECOND_OF_MINUTE) ? accessor.get(SECOND_OF_MINUTE) : 0;
		// complement nano_of_second with 0
		int nanoOfSecond =accessor.isSupported(NANO_OF_SECOND) ? accessor.get(NANO_OF_SECOND) : 0;
		LocalDateTime ldt =LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond);
		return TimestampData.fromLocalDateTime(ldt);
	} catch (DateTimeParseException e) {
		// fall back to support cases like '1999-9-10 05:20:10' or '1999-9-10'
		try {
			dateStr = dateStr.trim();
			int space = dateStr.indexOf(' ');
			if (space >= 0) {
				Timestamp ts = Timestamp.valueOf(dateStr);
				return TimestampData.fromTimestamp(ts);
			} else {
				java.sql.Date dt = java.sql.Date.valueOf(dateStr);
				return TimestampData.fromLocalDateTime(
						LocalDateTime.of(dt.toLocalDate(), LocalTime.MIDNIGHT));
			}
		} catch (IllegalArgumentException ie) {
			return null;
		}
	}

}




// f112, 内置函数 如 md5, hex()等; 位于 flink-table-planner 包; 

org.apache.flink.table.codegen.calls.FunctionGenerator {

	// String functions 字符串; 可用看到 都是 HashCalcCallGen 来实现代码生产; 
  addSqlFunctionMethod(
    SUBSTRING,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.SUBSTRING.method)

	// Temporal functions 日期函数
  addSqlFunction(
    TIMESTAMP_DIFF,
    Seq(
      new GenericTypeInfo(classOf[TimeUnit]),
      SqlTimeTypeInfo.TIMESTAMP,
      SqlTimeTypeInfo.TIMESTAMP),
    new TimestampDiffCallGen)

  addSqlFunction(
    ScalarSqlFunctions.DATE_FORMAT,
    Seq(SqlTimeTypeInfo.TIMESTAMP, STRING_TYPE_INFO),
    new DateFormatCallGen
  )
  

	// Cryptographic Hash functions 加密哈希函数
  addSqlFunction(
    ScalarSqlFunctions.MD5,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("MD5")
  )

  addSqlFunction(
    ScalarSqlFunctions.SHA224,
    Seq(STRING_TYPE_INFO),
    new HashCalcCallGen("SHA-224")
  )


}









/** Flink1.15+(包括 flink1.17), TO_TIMESTAMP() 内置函数的实现类是 org.apache.flink.table.utils.DateTimeUtils
*
*/

org.apache.flink.table.utils.DateTimeUtils.parseTimestampData(String dateStr, int precision) {
	// DEFAULT_TIMESTAMP_FORMATTER yyyy-[MM][M]-[dd][d] [HH][H]:[mm][m]:[ss][s]
	return TimestampData.fromLocalDateTime(fromTemporalAccessor(DEFAULT_TIMESTAMP_FORMATTER.parse(dateStr), precision));
}









