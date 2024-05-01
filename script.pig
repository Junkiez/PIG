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
female_over_60_percentage = FOREACH (GROUP extra_data BY all) GENERATE AVG(cond);
res = FILTER female_over_60_percentage BY type == 'Less then 65kg: ';

DUMP female_over_60_percentage;
