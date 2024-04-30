-- Завантаження даних з файлу
data = LOAD 'hdfs://sandbox-hdp.hortonworks.com:8020/uhadoop/bootcamp5.csv' USING PigStorage(';') AS (
    index:int,
    age:int,
    gender:int,
    height:int,
    weight:float,
    ap_hi:int,
    ap_lo:int,
    cholesterol:int,
    gluc:int,
    smoke:int,
    alco:int,
    active:int,
    cardio:int
);

male_data = FILTER data BY gender == 1;
female_data = FILTER data BY gender == 2;

--avg_height = FOREACH ( GROUP data BY gender ) GENERATE group, AVG(data.height);

--female_over_60 = FILTER female_data BY age > 60 AND weight < 65;
--female_over_60_count = FOREACH (GROUP female_over_60 ALL) GENERATE COUNT(female_over_60) AS count;
--female_count = FOREACH (GROUP female_data ALL) GENERATE COUNT(female_data) AS count;
extra_data = FOREACH female_data GENERATE (age/21900) as cond;
--female_over_60_percentage = FOREACH joined_data GENERATE ($0 / $1) * 100 AS percentage;


-- DUMP avg_height;
-- DUMP female_over_60_percentage;
DUMP extra_data;

