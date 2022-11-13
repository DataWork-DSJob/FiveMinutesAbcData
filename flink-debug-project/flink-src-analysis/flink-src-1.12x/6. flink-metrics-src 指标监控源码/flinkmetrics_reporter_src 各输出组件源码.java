



/** PrometheusReporter & PrometheusPushGatewayReporter, 注册 Metrics 和 转行发送Prom数据到Pushgateway 
 * flink_metric-prometheus_1.9.1 src + simpleclient_pushgateway_0.3.0
 * 
 * notifyOfAddedMetric() 注册 metric: 创建1个Collector并注册到普米的 CollectorRegistry中;
 * report() 发送到pushgateway: 
 */

class PrometheusReporter extends AbstractPrometheusReporter {
	
	void notifyOfAddedMetric(metric, metricName, group) {// org.apache.flink.metrics.prometheus.AbstractPrometheusReporter
		// 将MetricGroup.variables中的key,value 分放到2个列表中; 
		for (final Map.Entry<String, String> dimension : group.getAllVariables().entrySet()) {
			dimensionKeys.add(CHARACTER_FILTER.filterCharacters(key.substring(1, key.length() - 1)));
			dimensionValues.add(labelValueCharactersFilter.filterCharacters(dimension.getValue()));
		}
		String scopedMetricName = getScopedName(metricName, group);{
			return SCOPE_PREFIX + getLogicalScope(group) + SCOPE_SEPARATOR + CHARACTER_FILTER.filterCharacters(metricName);
		}
		
		if (collectorsWithCountByMetricName.containsKey(scopedMetricName)) {
			
		} else {//默认这里, 没用缓存没用的话新建Collector并添加到 CollectorRegistry对象的2个Map; 
			// 1. 先将 flink Metric对象转行成 prometheus的Metric对象; 
			collector = createCollector(metric, dimensionKeys, dimensionValues, scopedMetricName, helpString);{
				if (metric instanceof Gauge || metric instanceof Counter || metric instanceof Meter) {
					collector = io.prometheus.client.Gauge
						.build()
						.name(scopedMetricName).help(helpString)
						.labelNames(toArray(dimensionKeys))
						.create();
				}
				return collector;
			}
			collector.register(); {// io.prometheus.client.Collector
				return register(CollectorRegistry.defaultRegistry);{
					// 所谓注册, 就是把 Collector对象添加到 CollectorRegistry对象的2个Map中: namesToCollectors, collectorsToNames
					registry.register(this);{// io.prometheus.client.CollectorRegistry
						List<String> names = collectorNames(m);
						for (String name : names) {
							namesToCollectors.put(name, m);
						}
						collectorsToNames.put(m, names);
					}
				}
			}
		}
		
		// 把flink Metric包装放进 普米的collector中, 这样report()时直接通过flink Metric采集数据; 
		addMetric(metric, dimensionValues, collector);{
			io.prometheus.client.Gauge.Child child = gaugeFrom( metric);{//org.apache.flink.metrics.prometheus.AbstractPrometheusReporter.gaugeFrom()
				return new io.prometheus.client.Gauge.Child() {
					@Override
					public double get() {
						return (double) counter.getCount();
					}
				};
			}
			if (metric instanceof Gauge) {
				
			} else if (metric instanceof Counter) {
				((io.prometheus.client.Gauge) collector).setChild(child, toArray(dimensionValues));
			} 
		}
		collectorsWithCountByMetricName.put(scopedMetricName, new AbstractMap.SimpleImmutableEntry<>(collector, count + 1));
		
	}
	
	PrometheusPushGatewayReporter.report(){
		// 调用普米的 PushGateway , 发送静态实例: defaultRegistry
		pushGateway.push(CollectorRegistry.defaultRegistry, job){ // io.prometheus.client.exporter.PushGateway
			doRequest(registry, job, null, "PUT");{//PushGateway.doRequest()
				String url = gatewayBaseURL + URLEncoder.encode(job, "UTF-8"); // http://bdnode102:9091/metrics/job/jaxFlinkJob124deefdc10dd661d5ec06c8829175ec
				HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
				connection.setReadTimeout(10 * MILLISECONDS_PER_SECOND);
				connection.connect();
				try {
					BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
					
					// 这里采集输出 各group的指标样本 
					Enumeration<Collector.MetricFamilySamples> metricFamilySamples = registry.metricFamilySamples();{// CollectorRegistry.metricFamilySamples()
						return new MetricFamilySamplesEnumeration();{
							this(Collections.<String>emptySet());{// new CollectorRegistry.MetricFamilySamplesEnumeration()
								this.includedNames = includedNames;
								// 初始化时是空, 这里把 CollectorRegistry.collectorsToNames: Map<Collector, List<String>> 中的所有 Collector 迭代器返回 
								collectorIter: Iterator<Collector> = includedCollectorIterator(includedNames); {
									if (includedNames.isEmpty()) {
										return collectors().iterator();{// CollectorRegistry.collectors()
											// CollectorRegistry中成员变量: Map<Collector, List<String>> collectorsToNames
											return new HashSet<Collector>(collectorsToNames.keySet());
										}
									} else {}
								}
								
								findNextElement();{//CollectorRegistry$MetricFamilySamplesEnumeration.findNextElement()
									// metricFamilySamples 一开始为null, 跳过下面; 
									while (metricFamilySamples != null && metricFamilySamples.hasNext()) {
										next = filter(metricFamilySamples.next());
										if (next != null) {
										  return;
										}
										
									}
									// 进入这里; 遍历所有(382)各Collector, 
									if (next == null) {
										while (collectorIter.hasNext()) { // 382各Collect
											// 返回Collector子类SimpleCollector的实现类: prometheus.client.Gauge, Histogram
											// io.prometheus.client.Histogram, io.prometheus.client.Gauge, Counter, Summary
											List<MetricFamilySamples> metricFamilySamplesList = collectorIter.next().collect(); {
												Gauge.collect(){
													List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>(children.size());
													for(Map.Entry<List<String>, Child> c: children.entrySet()) {
														// AbstractPrometheusReporter.gaugeFrom()中 new io.prometheus.client.Gauge.Child() 时赋值的; 
														Object value = c.getValue().get(); {// Gauge$Child.get()
															Object value = gauge.getValue(); {// 源码细节 详见后面 
																//对于KafkaConsumer_fetch_rate 指标, gauge 实现类: KafkaMetricWrapper: 
																flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper.getValue();
																org.apache.flink.runtime.io.network.metrics.OutputBufferPoolUsageGauge.getValue();
																MetricUtils.$AttributeGauge.getValue();
															}
														}
														Sample sample = new MetricFamilySamples.Sample(fullname, labelNames, c.getKey(), value);{
															this(name, labelNames, labelValues, value, null);{
																this.name = name; // name, 就是指标名,如:  flink_taskmanager_job_task_operator_KafkaConsumer_fetch_rate
																this.labelNames = labelNames; // labelNames, 包括jobId,taskId等11各 label列;
																this.labelValues = labelValues; // labelValues, 包括 11列标签的值; 
																this.value = value; // 指标值, 如 2.032323
																this.timestampMs = timestampMs;// 时间戳, 这里为空,由采集器赋值;
															}
														}
														samples.add(new MetricFamilySamples.Sample(fullname, labelNames, c.getKey(), value));
													}
													return familySamplesList(Type.GAUGE, samples);
												}
											}
											
											metricFamilySamples = metricFamilySamplesList.iterator();
											while (metricFamilySamples.hasNext()) {
												next = filter(metricFamilySamples.next());
												if (next != null) {
													return;
												}
											}
										}
									}
									
									
								}
							}
						}
					}
					// 正是这里, 打印输出成 Prom /OpenMetrics 格式的数据; 
					TextFormat.write004(writer, metricFamilySamples); {
						
					}
					
					writer.flush();
					writer.close();
					// 这是什么逻辑, 202,204也都抛 IOEx返回? 
					if (response != HttpURLConnection.HTTP_ACCEPTED) {
						throw new IOException("Response code from " + url + " was " + response);
					}
				} finally {
					connection.disconnect();
				}
			}
		}
	}
	
}


/** InfluxdbReporter 注册 Metrics 和 转行发送 InfluxDB数据 
 * 
 * notifyOfAddedMetric() 注册个flink metric: 添加到 influxdb.AbstractReporter 成变 gauges:Map<Gauge<?>, MetricInfo> 
 * report()	将成变gauges 通过buildReport()转行成 InfluxDB BatchPoints数据,并发送
 */

class InfluxdbReporter extends AbstractReporter<MeasurementInfo> implements Scheduled {
	
	void notifyOfAddedMetric(metric, metricName, group) {// org.apache.flink.metrics.influxdb.AbstractReporter
		final MetricInfo metricInfo = metricInfoProvider.getMetricInfo(metricName, group);
		else if (metric instanceof Gauge) {
			this.gauges.put((Gauge<?>) metric, metricInfo);
		}
		
	}
	
	void report(){ // InfluxdbReporter.report()
		// 把flink metrics 转行成 Influxdb 的数据格式
		BatchPoints report = buildReport();{// InfluxdbReporter.buildReport()
			Instant timestamp = Instant.now();
			BatchPoints.Builder report = BatchPoints.database(database);
			for (Map.Entry<Gauge<?>, MeasurementInfo> entry : gauges.entrySet()) {
				Point point = MetricMapper.map(entry.getValue(), timestamp, entry.getKey());{ // MetricMapper.map(info, timestamp, Gauge<?> gauge);
					Point.Builder builder = builder(info, timestamp);
					// 在这里采集的? 
					Object value = gauge.getValue();
					builder.addField("value", (Number) value);
					return builder.build();
				}
				report.point(point);
			}
			
			for (Map.Entry<Histogram, MeasurementInfo> entry : histograms.entrySet()) {
				report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
			}
			for (Map.Entry<Meter, MeasurementInfo> entry : meters.entrySet()) {
				report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
			}
			return report.build();
		}
		
		influxDB.write(report);
	}
	
}



/** Slf4jReporter, 日志打印
 * 同时接收metric和定时report:  class Slf4jReporter extends AbstractReporter implements Scheduled
 *
 */
 
Slf4jReporter.report(){
	tryReport();{//Slf4jReporter.tryReport()
		StringBuilder builder = new StringBuilder((int) (previousSize * 1.1));
		builder.append("-- Counters -------------------------------------------------------------------");
		for (Map.Entry<Counter, String> metric : counters.entrySet()) {
			builder.append(metric.getValue()).append(": ").append(metric.getKey().getCount())
					.append(lineSeparator);
		}
		
		builder.append("-- Gauges ---------------------------------------------------------------------");
		for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
			builder.append(metric.getValue()).append(": ").append(metric.getKey().getValue())
					.append(lineSeparator);
		}
		
		builder.append("-- Histograms -----------------------------------------------------------------");
		for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
			HistogramStatistics stats = metric.getKey().getStatistics();
			builder
				.append(metric.getValue()).append(": count=").append(stats.size())
				.append(", min=").append(stats.getMin())
				.append(", max=").append(stats.getMax())
				.append(", mean=").append(stats.getMean())
				.append(", stddev=").append(stats.getStdDev())
				.append(", p50=").append(stats.getQuantile(0.50))
				.append(", p75=").append(stats.getQuantile(0.75))
				.append(", p95=").append(stats.getQuantile(0.95))
				.append(", p98=").append(stats.getQuantile(0.98))
				.append(", p99=").append(stats.getQuantile(0.99))
				.append(", p999=").append(stats.getQuantile(0.999))
				.append(lineSeparator);
		}
		
		LOG.info(builder.toString());
		previousSize = builder.length();
	}
	
	// Slf4jReporter 接收flink metrics 的方法, 靠flink的 AbstractReporter 父类实现: 
	org.apache.flink.metrics.reporter.AbstractReporter.notifyOfAddedMetric(){
		String name = group.getMetricIdentifier(metricName, this);
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, name);
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, name);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, name);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, name);
			}
		}
		
		AbstractReporter 成员变量包含4类指标: Gauge, Counter,Histogram, Meter
		{
			protected final Map<Gauge<?>, String> gauges = new HashMap<>();
			protected final Map<Counter, String> counters = new HashMap<>();
			protected final Map<Histogram, String> histograms = new HashMap<>();
			protected final Map<Meter, String> meters = new HashMap<>();
		}
	}
	
}








