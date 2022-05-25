create table public.opensky
(
	id SERIAL,
	icao24 varchar(100),
	callsign varchar(100),
	origin_country varchar(100),
	time_position integer,
	last_contact integer,
	longitude float,
	latitude float,
	baro_altitude float,
	on_ground boolean,
	velocity float,
	true_track float,
	vertical_rate float,
	sensors integer,
	geo_altitude float,
	squawk varchar(100),
	spi boolean,
	position_source integer
);


select * from public.opensky

select origin_country, count( distinct icao24 ) from public.opensky group by origin_country
