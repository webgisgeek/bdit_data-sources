SET search_path = crosic, pg_catalog; 

CREATE VIEW calendar_20171119_20171125 AS (

SELECT DISTINCT ON (feed_id) service_id, monday,
    tuesday ,
    wednesday ,
    thursday ,
    friday ,
    saturday ,
    sunday ,
    start_date ,
    end_date,
    feed_id
FROM gtfs_raph.calendar_imp
WHERE '2017-11-19' BETWEEN start_date AND end_date AND 
'2017-11-25' BETWEEN start_date AND end_date
ORDER BY feed_id, end_date DESC
);


CREATE VIEW calendar_dates_20171119_20171125 AS (
SELECT 
    service_id,
    date_ ,
    exception_type
FROM gtfs_raph.calendar_dates_imp
INNER JOIN calendar_20171119_20171125 USING (feed_id, service_id)
);

CREATE VIEW routes_20171119_20171125 AS  (
SELECT 
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    route_type,
    route_url,
    route_color,
    route_text_color
FROM gtfs_raph.routes
INNER JOIN calendar_20171119_20171125 USING (feed_id)
);

CREATE VIEW shapes_geom_20171119_20171125 AS  (
SELECT 
    shape_id ,
    feed_id ,
    geom 
FROM gtfs_raph.shapes_geom
INNER JOIN calendar_20171119_20171125 USING (feed_id)
);

CREATE VIEW stop_times_20171119_20171125  AS  (
SELECT
    trip_id,
    arrival_time,
    departure_time,
    stop_id,
    stop_sequence,
    stop_headsign,
    pickup_type,
    drop_off_type,
    shape_dist_traveled
FROM gtfs_raph.stop_times
INNER JOIN calendar_20171119_20171125  USING (feed_id)
);

CREATE VIEW stops_20171119_20171125 AS  (
SELECT 
    stop_id,
    stop_code,
    stop_name,
    stop_desc,
    stop_lat,
    stop_lon,
    geom,
    zone_id,
    stop_url,
    location_type,
    parent_station,
    wheelchair_boarding
FROM gtfs_raph.stops
INNER JOIN calendar_20171119_20171125 USING (feed_id)
);


CREATE VIEW  trips_20171119_20171125 AS (
SELECT 
    route_id,
    service_id,
    trip_id,
    trip_headsign,
    trip_short_name,
    direction_id,
    block_id,
    shape_id,
    wheelchair_accessible
FROM gtfs_raph.trips
INNER JOIN calendar_20171119_20171125 USING (feed_id, service_id)
);





