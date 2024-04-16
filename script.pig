-- Завантаження даних з файлу
data = LOAD 'bootcamp5.csv' USING PigStorage(',') AS (
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
female_over_60 = FILTER female_data BY age > 60 AND weight < 65;
female_over_60_count = FOREACH (GROUP female_over_60 ALL) GENERATE COUNT(female_over_60) AS count;
female_count = FOREACH (GROUP female_data ALL) GENERATE COUNT(female_data) AS count;
female_over_60_percentage = FOREACH (GROUP female_over_60 ALL) GENERATE (COUNT(female_over_60) / COUNT(female_data)) * 100 AS percentage;

-- Виведення результатів
female_height_avg_output = FOREACH female_height_avg GENERATE 'Середній зріст жінок:', $0 AS height_avg;
female_over_60_percentage_output = FOREACH female_over_60_percentage GENERATE 'Відсоток жінок старших 60 років з вагою менше 65 кг:', $0 AS percentage;

-- Вивід результатів
final_output = UNION male_height_avg_output, female_height_avg_output, female_over_60_percentage_output;
DUMP final_output;
