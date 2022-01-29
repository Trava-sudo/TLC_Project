-- I could iterate this in a python script, similar to 'Average_dist_per_hour' to
-- append every table of the same type vehicle and create one for yellow taxis, one for green, and so on containing all months
CREATE TABLE `Yellow_Taxis`
    SELECT * FROM `Yellow_Taxis_2021-01`
    UNION
    SELECT * FROM `Yellow_Taxis_2021-02`
    UNION
    ......



-- To be applied since the import through python makes all non float variables text

ALTER TABLE `Yellow_Taxis_2021-01`
MODIFY tpep_pickup_datetime DATETIME;

ALTER TABLE `Yellow_Taxis_2021-01`
MODIFY tpep_dropoff_datetime DATETIME;

ALTER TABLE `Yellow_Taxis_2021-01`
RENAME COLUMN tpep_pickup_datetime TO pickup_datetime;

ALTER TABLE `Yellow_Taxis_2021-01`
RENAME COLUMN tpep_dropoff_datetime TO dropoff_datetime;

SELECT * from `Yellow_Taxis_2021-01`;

Select time_format(tpep_dropoff_datetime, '%Y %m %d %H %i %S'), time_format(tpep_pickup_datetime, '%Y %m %d %H %i %S') From `Yellow_Taxis_2021-01`;

ALTER TABLE `Yellow_Taxis_2021-01`
MODIFY tpep_pickup_datetime DATETIME;

ALTER TABLE `Yellow_Taxis_2021-01`
MODIFY tpep_dropoff_datetime DATETIME;

ALTER TABLE `Green_Taxis_2021-01`
RENAME COLUMN lpep_pickup_datetime TO pickup_datetime;

ALTER TABLE `Green_Taxis_2021-01`
RENAME COLUMN lpep_dropoff_datetime TO dropoff_datetime;

select SUM(timediff(dropoff_datetime, pickup_datetime)) as  "travel_time(hours)" from `Green_Taxis_2021-01`;

SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

RESTART MYSQL SERVICE;

SELECT DATE_FORMAT(pickup_datetime, '%H') as pickup_hr, COUNT(*) AS Cnt
FROM `Green_Taxis_2021-01`
GROUP BY DATE_FORMAT(pickup_datetime, '%H')
ORDER BY Cnt DESC;

Select DAYOFWEEK(pickup_datetime), COUNT(*) AS Cnt from `Green_Taxis_2021-01`
Where passenger_count = 1 and DATE_FORMAT(pickup_datetime, '%Y') = 2021     -- or any Year I want to check
Group by DAYOFWEEK(pickup_datetime)
ORDER BY Cnt ASC;

Select DAYOFWEEK(pickup_datetime), COUNT(*) AS Cnt from `Green_Taxis_2021-01`
Where passenger_count = 1 and DATE_FORMAT(pickup_datetime, '%Y') = 2019     -- or any Year I want to check
Group by DAYOFWEEK(pickup_datetime)
ORDER BY Cnt ASC;

Select DATE_FORMAT(pickup_datetime, '%H') as hour_pickup, count(*) as Counts from `Green_Taxis_2021-01`
group by DATE_FORMAT(pickup_datetime, '%H')
order by Counts desc limit 3;
