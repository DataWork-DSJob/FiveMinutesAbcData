
INSERT INTO mall_interface_count_latency_1min
SELECT
    'myLogType' AS `logTypeName`,
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS+08:00') AS `timestamp`,
    `source`,
    `offset`,
    Map['appprogramname', appprogramname,'appsystem', appsystem, 'funcid', funcid, 'hostname', hostname] AS `dimensions`,
    Map['num', num, 'avgcost', avgcost] AS `measures`,
    Map['message', message, 'collecttime', collecttime] AS `normalFields`
FROM(

)
;


