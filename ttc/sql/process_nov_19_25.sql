-- 504 
-- step 1
SELECT message_datetime AS date_time, route, run, vehicle, latitude, longitude, position
INTO crosic.cis_504_nov
FROM ttc.cis_2017
WHERE message_datetime >= '2017-11-19 00:00:00' and message_datetime < '2017-11-26 00:00:00' and route = 504;


-- step 2
WITH geo_frame AS (
SELECT ST_GeomFromText (
'POLYGON((-79.53854563237667 43.58741045774194,-79.50009348393917 43.59586487156128,-79.49674608708858 43.60065103517571,-79.49271204473018 43.60220490253136,
-79.48601725102901 43.60829567555957,-79.48464396001339 43.61767923091661,-79.47365763188839 43.629422147616,-79.46661951543331 43.634516244547484,
-79.45752146245479 43.63737371962997,-79.45074083806514 43.63675254095344,-79.44499018193721 43.6353859252612,-79.43314554692745 43.63091314750915,
-79.41460611821651 43.62917364403781,-79.40464975835323 43.63414352038869,-79.37941553594112 43.638616057718274,-79.3589878320837 43.64433048208388,
-79.34851648808956 43.63277684536143,-79.31435587407589 43.658489973755195,-79.27744867803096 43.671652821766116,-79.25727846623897 43.695115266992175,
-79.23627140523433 43.704578338242776,-79.1932916879797 43.74232267388661,-79.23591735364437 43.84004639555846,-79.61431267262935 43.75463018484762,
-79.53854563237667 43.58741045774194))', 4326) AS frame
),

io AS (
SELECT *, ST_Within (position, frame) AS inside
FROM geo_frame, crosic.cis_504_nov
)

SELECT * INTO crosic.cis_nov_19_25_504
FROM io WHERE inside = TRUE;


-- step 3 
SELECT rank() OVER (order by date_time) AS rank_time,
        date_time, run, vehicle, latitude, longitude, position,
        degrees(ST_Azimuth(position, lag(position,1) OVER (partition by run order by date_time))) AS angle_previous,
        degrees(ST_Azimuth(position, lag(position,-1) OVER (partition by run order by date_time))) AS angle_next
    INTO crosic.cis_nov_504_angle
    FROM crosic.cis_nov_19_25_504;

-- step 5
WITH distinct_stop_patterns AS (SELECT DISTINCT ON (shape_id, stop_sequence) shape_id, direction_id, stop_sequence, stop_id
                                FROM trips_20171119_20171125
                                NATURAL JOIN stop_times_20171119_20171125
                                NATURAL JOIN  routes_20171119_20171125 
                                WHERE route_short_name = '504'
                                ORDER BY shape_id, stop_sequence)

SELECT shape_id, direction_id, stop_id, geom, stop_sequence
INTO crosic.nov_504_stop_pattern
FROM distinct_stop_patterns
NATURAL JOIN stops_20171119_20171125
ORDER BY shape_id, stop_sequence;

-- step 6
SELECT shape_id, direction_id, stop_id, geom, stop_sequence,
        degrees(ST_Azimuth(geom, lag(geom,1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_previous,
        degrees(ST_Azimuth(geom, lag(geom,-1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_next
INTO crosic.nov_504_stop_angle
FROM crosic.nov_504_stop_pattern; 

-- step 7 
ALTER TABLE cis_nov_504_angle
ADD COLUMN id SERIAL PRIMARY KEY,
ADD COLUMN stop_id integer, ADD COLUMN direction_id smallint; 

-- step 8 
UPDATE cis_nov_504_angle cis
SET stop_id = nearest.stop_id, direction_id = nearest.direction_id
FROM (SELECT b.id, stop_data.stop_id, stop_data.direction_id
      FROM cis_nov_504_angle b
      CROSS JOIN LATERAL
	(SELECT stop_id, direction_id
         FROM nov_504_stop_angle stops
         WHERE
         b.angle_previous IS NOT NULL
         AND
         ((stops.angle_previous IS NULL OR
           (b.angle_previous BETWEEN stops.angle_previous - 45 AND stops.angle_previous + 45))
           AND
           (b.angle_next IS NULL OR stops.angle_next IS NULL OR
           (b.angle_next BETWEEN stops.angle_next - 45 AND stops.angle_next + 45)))
        ORDER BY stops.geom <-> b.position LIMIT 1
        ) stop_data) nearest
WHERE nearest.id = cis.id; 


-- match stop issue 88 
DROP TABLE IF EXISTS match_stop_504_nov; 


DROP SEQUENCE IF EXISTS stops; 
CREATE SEQUENCE stops START 100; 

SELECT nextval('stops'); 

WITH line_data AS(
SELECT geom AS line, direction_id FROM shapes_geom_20171119_20171125
    INNER JOIN trips_20171119_20171125 USING (shape_id)
    WHERE shape_id IN (695070, 695071, 695072, 695075, 695077, 695078, 695081, 695084, 695085, 695092, 695093
)
    GROUP BY line, shape_id, direction_id
    ORDER BY shape_id
),

cis_gtfs AS(
SELECT date_time, id AS cis_id, stop_id, vehicle, a.direction_id,
ST_LineLocatePoint(line, position) AS cis_to_line,
ST_LineLocatePoint(line, geom) AS stop_to_line,
(CASE WHEN ST_LineLocatePoint(line, position) > ST_LineLocatePoint(line, geom)
      THEN 'after'
      WHEN ST_LineLocatePoint(line, position) < ST_LineLocatePoint(line, geom)
      THEN 'before'
      WHEN ST_LineLocatePoint(line, position) = ST_LineLocatePoint(line, geom)
      THEN 'same'
      END) AS line_position,
ST_Distance(position::geography, geom::geography) AS distance
FROM line_data a, cis_nov_504_angle b
INNER JOIN stops_20171119_20171125 USING (stop_id)
WHERE a.direction_id = b.direction_id
ORDER BY vehicle, a.direction_id, date_time
),

stop_orders AS (
SELECT *,
(CASE WHEN lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time) IS NULL
      THEN nextval('stops')
      WHEN stop_id <> lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN nextval('stops')
      WHEN stop_id = lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN currval('stops')
END) AS stop_order
FROM cis_gtfs
WHERE (line_position = 'before' AND distance <= 200) OR (line_position = 'after' AND distance <= 10) OR (line_position = 'same' AND distance <= 100)
ORDER BY vehicle, direction_id, date_time
)

SELECT MIN(date_time) AS arrival_time, MAX(date_time) AS departure_time, vehicle, stop_id, direction_id, array_agg(DISTINCT cis_id) AS cis_group
INTO match_stop_504_nov
FROM stop_orders
GROUP BY stop_order, vehicle, stop_id, direction_id; 



-- assign trip ids to cis data 

SELECT nextval('cis_lst'); 

WITH
order_data AS (
SELECT arrival_time, departure_time, vehicle, stop_id, direction_id, cis_group,
rank() OVER (PARTITION BY vehicle ORDER BY arrival_time) AS order_id
FROM match_stop_504_nov

ORDER BY vehicle, arrival_time
),

trips AS(
SELECT
(CASE WHEN lag(vehicle, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time) IS NULL
      THEN nextval('cis_lst')
      WHEN (direction_id <> lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN nextval('cis_lst')
      WHEN (direction_id = lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN currval('cis_lst')
END) AS trip_id,
arrival_time, departure_time, cis_group, direction_id, vehicle, stop_id
FROM order_data
ORDER BY vehicle, arrival_time
),

open_array AS (
SELECT trip_id, arrival_time, departure_time, unnest(cis_group) AS cis_id, direction_id, vehicle, stop_id
FROM trips
ORDER BY vehicle, arrival_time
),

good_trip_id AS(
SELECT trip_id, count(*), array_agg(cis_id) AS groups
FROM open_array
GROUP BY trip_id
HAVING count(*) > 10
ORDER BY count DESC)

SELECT a.trip_id, a.arrival_time, a.departure_time, a.cis_id, a.direction_id, a.vehicle, a.stop_id
INTO trips_cis_504_nov
FROM open_array a, good_trip_id b
WHERE a.trip_id = b.trip_id; 


-- trip filtering issue 87 
-- step 1 
WITH geo_frame AS (
SELECT ST_GeomFromText (
'POLYGON((-79.40035615335398 43.63780813971652,-79.40593514810496 43.65184496379264,-79.38997064004832 43.655074211367626,
-79.37452111612254 43.658241189360304,-79.36928544412547 43.64513751014725,-79.40035615335398 43.63780813971652))', 4326) AS frame
),

trip_location AS (
SELECT a.*, b.position FROM trips_cis_504 a
INNER JOIN cis_nov_504_angle b ON a.cis_id = b.id
)

SELECT trip_id, arrival_time, departure_time, cis_id, direction_id, vehicle, stop_id, ST_Within(position, frame) AS pilot_area
INTO trips_inpilot_cis_504_nov
FROM geo_frame, trip_location; 

-- step 2
WITH trips AS(
SELECT trip_id, arrival_time, departure_time, a.direction_id, a.vehicle, a.stop_id, pilot_area,
ST_DistanceSphere(position,
lag(position,1) OVER (partition by trip_id order by arrival_time)) AS distance
FROM trips_inpilot_cis_504_nov a
INNER JOIN cis_nov_504_angle b ON (a.cis_id = b.id)
WHERE pilot_area = TRUE
),

total_d AS (
SELECT trip_id, MIN(arrival_time) AS begin_time, MAX(departure_time) AS end_time, direction_id,
vehicle, SUM(distance)/1000 AS total_distance_km
FROM trips
GROUP BY trip_id, direction_id, vehicle
ORDER BY trip_id, begin_time
),

fail_trip_id AS (
SELECT trip_id
FROM total_d
WHERE total_distance_km < 1 OR total_distance_km > 4
)

DELETE FROM trips_inpilot_cis_504_nov a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id; 

-- step 3 
WITH trips AS(
SELECT trip_id, arrival_time, departure_time, a.direction_id, a.vehicle, a.stop_id, pilot_area
FROM trips_inpilot_cis_504_nov a
INNER JOIN cis_nov_504_angle b ON (a.cis_id = b.id)
WHERE pilot_area = TRUE
),

total_time AS (
SELECT trip_id, MIN(arrival_time) AS begin_time, MAX(departure_time) AS end_time,
EXTRACT(EPOCH FROM (MAX(departure_time) - MIN(arrival_time)))/60 AS time_diff,
direction_id, vehicle
FROM trips
GROUP BY trip_id, direction_id, vehicle
ORDER BY trip_id, begin_time
),

fail_trip_id AS (
SELECT trip_id
FROM total_time
WHERE time_diff > 100
)

DELETE FROM trips_inpilot_cis_504_nov a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id; 

-- step 4
WITH fail_trip_id AS(
SELECT trip_id
FROM trips_inpilot_cis_504_nov a
INNER JOIN cis_nov_504_angle b ON (a.cis_id = b.id)
WHERE run BETWEEN 60 AND 89
)

DELETE FROM trips_inpilot_cis_504_nov a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id; 



 












-- 514 


--step 1
SELECT message_datetime AS date_time, route, run, vehicle, latitude, longitude, position
INTO crosic.cis_514_nov
FROM ttc.cis_2017
WHERE message_datetime >= '2017-11-19 00:00:00' and message_datetime < '2017-11-26 00:00:00' and route = 514;


-- step 2
WITH geo_frame AS (
SELECT ST_GeomFromText (
'POLYGON((-79.53854563237667 43.58741045774194,-79.50009348393917 43.59586487156128,-79.49674608708858 43.60065103517571,-79.49271204473018 43.60220490253136,
-79.48601725102901 43.60829567555957,-79.48464396001339 43.61767923091661,-79.47365763188839 43.629422147616,-79.46661951543331 43.634516244547484,
-79.45752146245479 43.63737371962997,-79.45074083806514 43.63675254095344,-79.44499018193721 43.6353859252612,-79.43314554692745 43.63091314750915,
-79.41460611821651 43.62917364403781,-79.40464975835323 43.63414352038869,-79.37941553594112 43.638616057718274,-79.3589878320837 43.64433048208388,
-79.34851648808956 43.63277684536143,-79.31435587407589 43.658489973755195,-79.27744867803096 43.671652821766116,-79.25727846623897 43.695115266992175,
-79.23627140523433 43.704578338242776,-79.1932916879797 43.74232267388661,-79.23591735364437 43.84004639555846,-79.61431267262935 43.75463018484762,
-79.53854563237667 43.58741045774194))', 4326) AS frame
),

io AS (
SELECT *, ST_Within (position, frame) AS inside
FROM geo_frame, crosic.cis_514_nov
)

SELECT * INTO crosic.cis_nov_19_25_514
FROM io WHERE inside = TRUE;


-- step 3
SELECT rank() OVER (order by date_time) AS rank_time,
        date_time, run, vehicle, latitude, longitude, position,
        degrees(ST_Azimuth(position, lag(position,1) OVER (partition by run order by date_time))) AS angle_previous,
        degrees(ST_Azimuth(position, lag(position,-1) OVER (partition by run order by date_time))) AS angle_next
    INTO crosic.cis_nov_514_angle
    FROM crosic.cis_nov_19_25_514;
	
-- step 5
WITH distinct_stop_patterns AS (SELECT DISTINCT ON (shape_id, stop_sequence) shape_id, direction_id, stop_sequence, stop_id
                                FROM trips_20171119_20171125
                                NATURAL JOIN stop_times_20171119_20171125
                                NATURAL JOIN  routes_20171119_20171125 
                                WHERE route_short_name = '514'
                                ORDER BY shape_id, stop_sequence)

SELECT shape_id, direction_id, stop_id, geom, stop_sequence
INTO crosic.nov_514_stop_pattern
FROM distinct_stop_patterns
NATURAL JOIN stops_20171119_20171125
ORDER BY shape_id, stop_sequence;

-- step 6
SELECT shape_id, direction_id, stop_id, geom, stop_sequence,
        degrees(ST_Azimuth(geom, lag(geom,1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_previous,
        degrees(ST_Azimuth(geom, lag(geom,-1) OVER (partition by shape_id, direction_id order by stop_sequence))) AS angle_next
INTO crosic.nov_514_stop_angle
FROM crosic.nov_514_stop_pattern; 

-- step 7
ALTER TABLE cis_nov_514_angle
ADD COLUMN id SERIAL PRIMARY KEY,
ADD COLUMN stop_id integer, ADD COLUMN direction_id smallint; 

-- step 8 
UPDATE cis_nov_514_angle cis
SET stop_id = nearest.stop_id, direction_id = nearest.direction_id
FROM (SELECT b.id, stop_data.stop_id, stop_data.direction_id
      FROM cis_nov_514_angle b
      CROSS JOIN LATERAL
	(SELECT stop_id, direction_id
         FROM nov_514_stop_angle stops
         WHERE
         b.angle_previous IS NOT NULL
         AND
         ((stops.angle_previous IS NULL OR
           (b.angle_previous BETWEEN stops.angle_previous - 45 AND stops.angle_previous + 45))
           AND
           (b.angle_next IS NULL OR stops.angle_next IS NULL OR
           (b.angle_next BETWEEN stops.angle_next - 45 AND stops.angle_next + 45)))
        ORDER BY stops.geom <-> b.position LIMIT 1
        ) stop_data) nearest
WHERE nearest.id = cis.id; 



-- match stop issue 88 
DROP SEQUENCE IF EXISTS stops; 
CREATE SEQUENCE stops START 100; 

SELECT nextval('stops'); 

WITH line_data AS(
SELECT geom AS line, direction_id FROM shapes_geom_20171119_20171125
    INNER JOIN trips_20171119_20171125 USING (shape_id)
    WHERE shape_id IN (695251, 695252, 695253, 695254)
    GROUP BY line, shape_id, direction_id
    ORDER BY shape_id
),

cis_gtfs AS(
SELECT date_time, id AS cis_id, stop_id, vehicle, a.direction_id,
ST_LineLocatePoint(line, position) AS cis_to_line,
ST_LineLocatePoint(line, geom) AS stop_to_line,
(CASE WHEN ST_LineLocatePoint(line, position) > ST_LineLocatePoint(line, geom)
      THEN 'after'
      WHEN ST_LineLocatePoint(line, position) < ST_LineLocatePoint(line, geom)
      THEN 'before'
      WHEN ST_LineLocatePoint(line, position) = ST_LineLocatePoint(line, geom)
      THEN 'same'
      END) AS line_position,
ST_Distance(position::geography, geom::geography) AS distance
FROM line_data a, cis_nov_514_angle b
INNER JOIN stops_20171119_20171125 USING (stop_id)
WHERE a.direction_id = b.direction_id
ORDER BY vehicle, a.direction_id, date_time
),

stop_orders AS (
SELECT *,
(CASE WHEN lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time) IS NULL
      THEN nextval('stops')
      WHEN stop_id <> lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN nextval('stops')
      WHEN stop_id = lag(stop_id, 1) OVER (PARTITION BY vehicle ORDER BY date_time)
      THEN currval('stops')
END) AS stop_order
FROM cis_gtfs
WHERE (line_position = 'before' AND distance <= 200) OR (line_position = 'after' AND distance <= 10) OR (line_position = 'same' AND distance <= 100)
ORDER BY vehicle, direction_id, date_time
)

SELECT MIN(date_time) AS arrival_time, MAX(date_time) AS departure_time, vehicle, stop_id, direction_id, array_agg(DISTINCT cis_id) AS cis_group
INTO match_stop_514_nov
FROM stop_orders
GROUP BY stop_order, vehicle, stop_id, direction_id; 


-- assign trip ids to cis data 

DROP SEQUENCE IF EXISTS cis_lst; 
CREATE SEQUENCE cis_lst START 100; 
SELECT nextval('cis_lst'); 

WITH
order_data AS (
SELECT arrival_time, departure_time, vehicle, stop_id, direction_id, cis_group,
rank() OVER (PARTITION BY vehicle ORDER BY arrival_time) AS order_id
FROM match_stop_514_nov

ORDER BY vehicle, arrival_time
),

trips AS(
SELECT
(CASE WHEN lag(vehicle, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time) IS NULL
      THEN nextval('cis_lst')
      WHEN (direction_id <> lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN nextval('cis_lst')
      WHEN (direction_id = lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN currval('cis_lst')
END) AS trip_id,
arrival_time, departure_time, cis_group, direction_id, vehicle, stop_id
FROM order_data
ORDER BY vehicle, arrival_time
),

open_array AS (
SELECT trip_id, arrival_time, departure_time, unnest(cis_group) AS cis_id, direction_id, vehicle, stop_id
FROM trips
ORDER BY vehicle, arrival_time
),

good_trip_id AS(
SELECT trip_id, count(*), array_agg(cis_id) AS groups
FROM open_array
GROUP BY trip_id
HAVING count(*) > 10
ORDER BY count DESC)

SELECT a.trip_id, a.arrival_time, a.departure_time, a.cis_id, a.direction_id, a.vehicle, a.stop_id
INTO trips_cis_514_nov
FROM open_array a, good_trip_id b
WHERE a.trip_id = b.trip_id; 


-- trip filtering issue 87 
-- step 1 
WITH geo_frame AS (
SELECT ST_GeomFromText (
'POLYGON((-79.40035615335398 43.63780813971652,-79.40593514810496 43.65184496379264,-79.38997064004832 43.655074211367626,
-79.37452111612254 43.658241189360304,-79.36928544412547 43.64513751014725,-79.40035615335398 43.63780813971652))', 4326) AS frame
),

trip_location AS (
SELECT a.*, b.position FROM trips_cis_514 a
INNER JOIN cis_nov_514_angle b ON a.cis_id = b.id
)

SELECT trip_id, arrival_time, departure_time, cis_id, direction_id, vehicle, stop_id, ST_Within(position, frame) AS pilot_area
INTO trips_inpilot_cis_514_nov
FROM geo_frame, trip_location; 

-- step 2
WITH trips AS(
SELECT trip_id, arrival_time, departure_time, a.direction_id, a.vehicle, a.stop_id, pilot_area,
ST_DistanceSphere(position,
lag(position,1) OVER (partition by trip_id order by arrival_time)) AS distance
FROM trips_inpilot_cis_514_nov a
INNER JOIN cis_nov_514_angle b ON (a.cis_id = b.id)
WHERE pilot_area = TRUE
),

total_d AS (
SELECT trip_id, MIN(arrival_time) AS begin_time, MAX(departure_time) AS end_time, direction_id,
vehicle, SUM(distance)/1000 AS total_distance_km
FROM trips
GROUP BY trip_id, direction_id, vehicle
ORDER BY trip_id, begin_time
),

fail_trip_id AS (
SELECT trip_id
FROM total_d
WHERE total_distance_km < 1 OR total_distance_km > 4
)

DELETE FROM trips_inpilot_cis_514_nov a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id; 

-- step 3
WITH trips AS(
SELECT trip_id, arrival_time, departure_time, a.direction_id, a.vehicle, a.stop_id, pilot_area
FROM trips_inpilot_cis_514_nov a
INNER JOIN cis_nov_514_angle b ON (a.cis_id = b.id)
WHERE pilot_area = TRUE
),

total_time AS (
SELECT trip_id, MIN(arrival_time) AS begin_time, MAX(departure_time) AS end_time,
EXTRACT(EPOCH FROM (MAX(departure_time) - MIN(arrival_time)))/60 AS time_diff,
direction_id, vehicle
FROM trips
GROUP BY trip_id, direction_id, vehicle
ORDER BY trip_id, begin_time
),

fail_trip_id AS (
SELECT trip_id
FROM total_time
WHERE time_diff > 100
)

DELETE FROM trips_inpilot_cis_514_nov a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id; 


-- step 4 
WITH fail_trip_id AS(
SELECT trip_id
FROM trips_inpilot_cis_514_nov a
INNER JOIN cis_nov_514_angle b ON (a.cis_id = b.id)
WHERE run BETWEEN 60 AND 89
)

DELETE FROM trips_inpilot_cis_514_nov a
USING fail_trip_id
WHERE a.trip_id = fail_trip_id.trip_id; 
























-- validating cis create trip_ids thing 

CREATE TEMPORARY SEQUENCE tripid_seq;
SELECT setval('tripid_seq', 1);

WITH trip_ids AS (
SELECT arrival_time, departure_time, vehicle, stop_id, direction_id, cis_group,
rank() OVER (PARTITION BY vehicle ORDER BY arrival_time) AS order_id
FROM crosic.match_stop_504_nov
ORDER BY vehicle, arrival_time 
)

SELECT
(CASE WHEN lag(vehicle, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time) IS NULL
      THEN nextval('tripid_seq')
      WHEN (direction_id <> lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN nextval('tripid_seq')
      WHEN (direction_id = lag(direction_id, 1) OVER (PARTITION BY vehicle ORDER BY arrival_time))
      THEN currval('tripid_seq')
END) AS trip_id,
arrival_time, departure_time, cis_group, direction_id, vehicle, stop_id
INTO crosic.cis_504_11192017_11252017_tripids
FROM trip_ids
ORDER BY vehicle, arrival_time;

DROP SEQUENCE tripid_seq;


