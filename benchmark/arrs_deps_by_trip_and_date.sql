SELECT * from bench(
'SELECT *
FROM arrivals_departures
WHERE trip_id = ''168977951''
AND date > ''2022-08-08'' AND date <= ''2022-08-09'''
);
