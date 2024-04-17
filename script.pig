-- Завантаження даних з файлу
data = LOAD 'hdfs://sandbox-hdp.hortonworks.com:8020/uhadoop/itsymbaliuk/bootcamp5.csv' USING PigStorage(',') AS (
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

-- Фільтруємо дані за віком та вагою для жінок
out = FILTER female_data BY age > 60 AND weight < 65;

DUMP out;
