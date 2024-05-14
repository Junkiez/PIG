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

--Task 1
avg_height = FOREACH ( GROUP data BY gender ) GENERATE group, AVG(data.height);

--Task 2
female_over_60 = FILTER female_data BY age > 60;
-- female_over_60_count = FOREACH (GROUP female_over_60 ALL) GENERATE COUNT(female_over_60) AS count;
extra_data = FOREACH female_over_60 GENERATE ((weight < 65) ? 1 : 0) as cond;
female_over_60_percentage = FOREACH (GROUP extra_data ALL) GENERATE AVG(extra_data.cond) AS av;

male_data = FILTER data BY gender == 1 AND cardio == 0;
extra_data = FOREACH male_data GENERATE ((weight > 90) ? 1 : 0) as cond;
result = FOREACH (GROUP extra_data ALL) GENERATE SIZE(extra_data.cond) AS av;
--res = FILTER female_over_60_percentage BY type == 'Less then 65kg: ';

DUMP result;
