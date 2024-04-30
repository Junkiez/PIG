-- Завантаження даних з файлу
data = LOAD 'hdfs://sandbox-hdp.hortonworks.com:8020/uhadoop/downloaded-logs.csv' USING PigStorage(',') AS (
    latency:chararray,
    protocol:chararray,
    referer:chararray,
    remoteIp:chararray,
    requestMethod:chararray,
    requestSize:int,
    requestUrl:chararray,
    responseSize:int,
    serverIp:chararray,
    status:chararray,
    userAgent:chararray,
    insertId:chararray,
    commit_sha:chararray,
    gcb_build_id:chararray,
    gcb_trigger_id:chararray,
    gcb_trigger_region:chararray,
    instanceId:chararray,
    managed_by:chararray,
    logName:chararray,
    receiveLocation:chararray,
    receiveTimestamp:chararray,
    configuration_name:chararray,
    location:chararray,
    project_id:chararray,
    revision_name:chararray,
    service_name:chararray,
    resource_type:chararray,
    severity:chararray,
    spanId:chararray,
    textPayload:chararray,
    timestamp:chararray,
    trace:chararray,
    traceSampled:chararray
);

--male_data = FILTER data BY gender == 1;
--female_data = FILTER data BY gender == 2;

clean_data = FOREACH data GENERATE requestMethod, status, latency, remoteIp, severity, userAgent, timestamp ;

--Task 1
--avg_height = FOREACH ( GROUP data BY gender ) GENERATE group, AVG(data.height);

--Task 2
--female_over_60 = FILTER female_data BY age > 60;
--female_over_60_count = FOREACH (GROUP female_over_60 ALL) GENERATE COUNT(female_over_60) AS count;
--extra_data = FOREACH female_over_60 GENERATE ((weight < 65) ? 1 : 0) as cond;
--female_over_60_percentage = FOREACH (GROUP extra_data BY cond) GENERATE (group == 1 ? 'Less then 65kg: ' : 'More then 65kg: ') AS type, COUNT(extra_data)/(double)female_over_60_count.count * 100 as count, '%' AS sym;
--res = FILTER female_over_60_percentage BY type == 'Less then 65kg: ';

DUMP clean_data;
