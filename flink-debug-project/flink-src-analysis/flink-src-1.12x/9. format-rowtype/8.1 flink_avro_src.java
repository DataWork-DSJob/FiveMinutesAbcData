
// DirectConvertRule 建立函数名和实现类的映射关系

FlinkSqlOperatorTable {
        PROCTIME
        UNIX_TIMESTAMP
        TO_TIMESTAMP
        TO_DATE
        STREAMRECORD_TIMESTAMP
}

DirectConvertRule {
    static {
        DEFINITION_OPERATOR_MAP.put( BuiltInFunctionDefinitions.DATE_FORMAT, FlinkSqlOperatorTable.DATE_FORMAT);

    }

}


// 2. 实现类

   Schema AvroSchemaConverter.convertToSchema(LogicalType logicalType, String rowName) {
        boolean nullable = logicalType.isNullable();
        int precision;
        switch(logicalType.getTypeRoot()) {
        case NULL:
            return (Schema)SchemaBuilder.builder().nullType();
        case BOOLEAN:
            Schema bool = (Schema)SchemaBuilder.builder().booleanType();
            return nullable ? nullableSchema(bool) : bool;
        case TINYINT:
        case SMALLINT:
        case INTEGER:
            Schema integer = (Schema)SchemaBuilder.builder().intType();
            return nullable ? nullableSchema(integer) : integer;
        case BIGINT:
            Schema bigint = (Schema)SchemaBuilder.builder().longType();
            return nullable ? nullableSchema(bigint) : bigint;
        case FLOAT:
            Schema f = (Schema)SchemaBuilder.builder().floatType();
            return nullable ? nullableSchema(f) : f;
        case DOUBLE:
            Schema d = (Schema)SchemaBuilder.builder().doubleType();
            return nullable ? nullableSchema(d) : d;
        case CHAR:
        case VARCHAR:
            Schema str = (Schema)SchemaBuilder.builder().stringType();
            return nullable ? nullableSchema(str) : str;
        case BINARY:
        case VARBINARY:
            Schema binary = (Schema)SchemaBuilder.builder().bytesType();
            return nullable ? nullableSchema(binary) : binary;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
            TimestampType timestampType = (TimestampType)logicalType;
            precision = timestampType.getPrecision();
            if (precision <= 3) {
                LogicalType avroLogicalType = LogicalTypes.timestampMillis();
                Schema timestamp = avroLogicalType.addToSchema((Schema)SchemaBuilder.builder().longType());
                return nullable ? nullableSchema(timestamp) : timestamp;
            }

            throw new IllegalArgumentException("Avro does not support TIMESTAMP type with precision: " + precision + ", it only supports precision less than 3.");
        case DATE:
            Schema date = LogicalTypes.date().addToSchema((Schema)SchemaBuilder.builder().intType());
            return nullable ? nullableSchema(date) : date;
        case TIME_WITHOUT_TIME_ZONE:
            precision = ((TimeType)logicalType).getPrecision();
            if (precision > 3) {
                throw new IllegalArgumentException("Avro does not support TIME type with precision: " + precision + ", it only supports precision less than 3.");
            }

            Schema time = LogicalTypes.timeMillis().addToSchema((Schema)SchemaBuilder.builder().intType());
            return nullable ? nullableSchema(time) : time;
        case DECIMAL:
            DecimalType decimalType = (DecimalType)logicalType;
            Schema decimal = LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale()).addToSchema((Schema)SchemaBuilder.builder().bytesType());
            return nullable ? nullableSchema(decimal) : decimal;
        case ROW:
            RowType rowType = (RowType)logicalType;
            List<String> fieldNames = rowType.getFieldNames();
            FieldAssembler<Schema> builder = SchemaBuilder.builder().record(rowName).fields();

            for(int i = 0; i < rowType.getFieldCount(); ++i) {
                String fieldName = (String)fieldNames.get(i);
                org.apache.flink.table.types.logical.LogicalType fieldType = rowType.getTypeAt(i);
                GenericDefault<Schema> fieldBuilder = builder.name(fieldName).type(convertToSchema(fieldType, rowName + "_" + fieldName));
                if (fieldType.isNullable()) {
                    builder = fieldBuilder.withDefault((Object)null);
                } else {
                    builder = fieldBuilder.noDefault();
                }
            }

            Schema record = (Schema)builder.endRecord();
            return nullable ? nullableSchema(record) : record;
        case MULTISET:
        case MAP:
            Schema map = (Schema)SchemaBuilder.builder().map().values(convertToSchema(extractValueTypeToAvroMap(logicalType), rowName));
            return nullable ? nullableSchema(map) : map;
        case ARRAY:
            ArrayType arrayType = (ArrayType)logicalType;
            Schema array = (Schema)SchemaBuilder.builder().array().items(convertToSchema(arrayType.getElementType(), rowName));
            return nullable ? nullableSchema(array) : array;
        case RAW:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        default:
            throw new UnsupportedOperationException("Unsupported to derive Schema for type: " + logicalType);
        }
    }


