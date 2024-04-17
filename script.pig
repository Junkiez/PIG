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

female_data = FILTER data BY gender == 2;

-- Фільтруємо дані за віком та вагою для жінок
female_over_60 = FILTER female_data BY age > 60 AND weight < 65;
-- Обчислення кількості записів жінок старших 60 років
female_over_60_count = FOREACH (GROUP female_over_60 ALL) GENERATE COUNT(female_over_60) AS count;
-- Обчислення кількості всіх записів про жінок
female_count = FOREACH (GROUP female_data ALL) GENERATE COUNT(female_data) AS count;
-- Обчислення відсотка жінок старших 60 років з вагою менше 65 кг
joined_data = JOIN female_over_60_count BY count, female_count BY count;
-- Обчислення відсотка жінок старших 60 років з вагою менше 65 кг
female_over_60_percentage = FOREACH joined_data GENERATE ($0 / $1) * 100 AS percentage;

-- Виведення результатів
female_over_60_percentage_output = FOREACH female_over_60_percentage GENERATE 'Відсоток жінок старших 60 років з вагою менше 65 кг:', $0 AS percentage;

STORE female_data INTO 'hdfs://sandbox-hdp.hortonworks.com:8020/uhadoop/itsymbaliuk/results';
