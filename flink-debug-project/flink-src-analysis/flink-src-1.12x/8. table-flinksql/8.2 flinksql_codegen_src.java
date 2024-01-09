
// 将 sql 转换 Stream api的 java代码 : GeneratedOperator.code 

TableEnvironmentImpl.executeSql() {
	List<Transformation<?>> transformations = this.translate(Collections.singletonList(sinkOperation));
		StreamPlanner.translateToPlan()
			StreamExecLegacySink.translateToPlan()
				StreamExecCalcBase.translateToPlan()
					StreamExecCalc.translateToPlanInternal(planner);
					
	StreamExecCalc.translateToPlanInternal(planner: StreamPlanner): Transformation[RowData] = {
		val inputTransform = getInputNodes.get(0).translateToPlan(planner).asInstanceOf[Transformation[RowData]]
		
		val ctx = CodeGeneratorContext(config).setOperatorBaseClass(classOf[AbstractProcessStreamOperator[RowData]])
		val outputType = FlinkTypeFactory.toLogicalRowType(getRowType);
		// 这里是生成代码类的关键; 
		val substituteStreamOperator = CalcCodeGenerator.generateCalcOperator(ctx,inputTransform,);{
			
			val processCode = generateProcessCode();
			// 这里是生成1个输出的算子; GeneratedOperator.code 就是生成的类代码; 
			val genOperator: GeneratedOperator = OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](); {
				
				val inputTypeTerm = boxedTypeTermForType(inputType)
				
    val operatorCode =
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName}$endInputImpl {

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $operatorName(
            Object[] references,
            ${className[StreamTask[_, _]]} task,
            ${className[StreamConfig]} config,
            ${className[Output[_]]} output,
            ${className[ProcessingTimeService]} processingTimeService) throws Exception {
          this.references = references;
          ${ctx.reuseInitCode()}
          this.setup(task, config, output);
          if (this instanceof ${className[AbstractStreamOperator[_]]}) {
            ((${className[AbstractStreamOperator[_]]}) this)
              .setProcessingTimeService(processingTimeService);
          }
        }

        @Override
        public void open() throws Exception {
          super.open();
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void processElement($STREAM_RECORD $ELEMENT) throws Exception {
          $inputTypeTerm $inputTerm = ($inputTypeTerm) ${converter(s"$ELEMENT.getValue()")};
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${if (lazyInputUnboxingCode) "" else ctx.reuseInputUnboxingCode()}
          $processCode
        }

        $endInput

        @Override
        public void close() throws Exception {
           super.close();
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

				LOG.debug(s"Compiling OneInputStreamOperator Code:\n$name")
				new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray)
				
			}
			return new CodeGenOperatorFactory(genOperator);
		}
		
		val ret = new OneInputTransformation();
		
	}

}


## 查询Sql 预计
	event_time AS TO_TIMESTAMP(FROM_UNIXTIME(time_sec))

	SELECT group_key, event_time, DATE_FORMAT(event_time, 'HH:mm:ss') as tim_millis, count(*) AS cnt, PROCTIME() AS query_time 
	FROM ods_trade_csv 
	GROUP BY group_key, event_time

## 生成的 java代码 类 , 重点是 
			SqlDateTimeUtils.fromUnixtime(field$9, timeZone)
			SqlDateTimeUtils.toTimestampData(result$11.toString())


      public class StreamExecCalc$14 extends org.apache.flink.table.runtime.operators.AbstractProcessStreamOperator
          implements org.apache.flink.streaming.api.operators.OneInputStreamOperator {

        private final Object[] references;
        private transient org.apache.flink.table.runtime.typeutils.StringDataSerializer typeSerializer$7;
        private static final java.util.TimeZone timeZone =
                         java.util.TimeZone.getTimeZone("Asia/Shanghai");
        org.apache.flink.table.data.BoxedWrapperRowData out = new org.apache.flink.table.data.BoxedWrapperRowData(2);
        private final org.apache.flink.streaming.runtime.streamrecord.StreamRecord outElement = new org.apache.flink.streaming.runtime.streamrecord.StreamRecord(null);

        public StreamExecCalc$14(
            Object[] references,
            org.apache.flink.streaming.runtime.tasks.StreamTask task,
            org.apache.flink.streaming.api.graph.StreamConfig config,
            org.apache.flink.streaming.api.operators.Output output,
            org.apache.flink.streaming.runtime.tasks.ProcessingTimeService processingTimeService) throws Exception {
          this.references = references;
          typeSerializer$7 = (((org.apache.flink.table.runtime.typeutils.StringDataSerializer) references[0]));
          this.setup(task, config, output);
          if (this instanceof org.apache.flink.streaming.api.operators.AbstractStreamOperator) {
            ((org.apache.flink.streaming.api.operators.AbstractStreamOperator) this)
              .setProcessingTimeService(processingTimeService);
          }
        }

        @Override
        public void open() throws Exception {
          super.open();
          
        }

        @Override
        public void processElement(org.apache.flink.streaming.runtime.streamrecord.StreamRecord element) throws Exception {
          org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) element.getValue();
          
          org.apache.flink.table.data.binary.BinaryStringData field$6;
          boolean isNull$6;
          org.apache.flink.table.data.binary.BinaryStringData field$8;
          long field$9;
          boolean isNull$9;
          boolean isNull$10;
          org.apache.flink.table.data.binary.BinaryStringData result$11;
          boolean isNull$12;
          org.apache.flink.table.data.TimestampData result$13;
          
          
          isNull$9 = in1.isNullAt(2);
          field$9 = -1L;
          if (!isNull$9) {
            field$9 = in1.getLong(2);
          }
          
          isNull$6 = in1.isNullAt(1);
          field$6 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
          if (!isNull$6) {
            field$6 = ((org.apache.flink.table.data.binary.BinaryStringData) in1.getString(1));
          }
          field$8 = field$6;
          if (!isNull$6) {
            field$8 = (org.apache.flink.table.data.binary.BinaryStringData) (typeSerializer$7.copy(field$8));
          }
                  
          
          out.setRowKind(in1.getRowKind());

          
          if (isNull$6) {
            out.setNullAt(0);
          } else {
            out.setNonPrimitiveValue(0, field$8);
          }
          
          isNull$10 = isNull$9;
          result$11 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
          if (!isNull$10) {
            
            result$11 = org.apache.flink.table.data.binary.BinaryStringData.fromString(
          org.apache.flink.table.runtime.functions.SqlDateTimeUtils.fromUnixtime(field$9, timeZone)
                     );
            isNull$10 = (result$11 == null);
          }
          
          isNull$12 = isNull$10;
          result$13 = null;
          if (!isNull$12) {
            
            result$13 = 
          org.apache.flink.table.runtime.functions.SqlDateTimeUtils.toTimestampData(result$11.toString())
                     ;
            isNull$12 = (result$13 == null);
          }
          
          if (isNull$12) {
            out.setNullAt(1);
          } else {
            out.setNonPrimitiveValue(1, result$13);
          }
                    
                  
          output.collect(outElement.replace(out));
          
          
        }


        @Override
        public void close() throws Exception {
           super.close();
          
        }

        
      }


SqlDateTimeUtils.fromUnixtime(field$9, timeZone)
SqlDateTimeUtils.toTimestampData(result$11.toString())



