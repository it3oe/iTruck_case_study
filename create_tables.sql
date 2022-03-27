CREATE TABLE IF NOT EXISTS weather_type (
	id INTEGER PRIMARY KEY,
	type_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_severity (
	id INTEGER PRIMARY KEY,
	severity_type VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS us_state (
	id INTEGER PRIMARY KEY,
	state_code CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS county (
	id INTEGER PRIMARY KEY,
	county_name VARCHAR(255) NOT NULL,
	state_id INTEGER NOT NULL,
	FOREIGN KEY (state_id) REFERENCES us_state (id)
);

CREATE TABLE IF NOT EXISTS city (
	id INTEGER PRIMARY KEY,
	city_name VARCHAR(255) NOT NULL,
	timezone VARCHAR(255) NOT NULL,
	county_id INTEGER NOT NULL,
	FOREIGN KEY (county_id) REFERENCES county (id)
);

CREATE TABLE IF NOT EXISTS station (
	id INTEGER PRIMARY KEY,
	airport_code CHAR(4) NOT NULL,
	latitude DECIMAL(9,6) NOT NULL,
	longitude DECIMAL(9,6) NOT NULL,
	zipcode INTEGER --NOT NULL,
	city_id INTEGER NOT NULL,
	FOREIGN KEY (city_id) REFERENCES city (id)
);

CREATE TABLE IF NOT EXISTS event (
	id INTEGER PRIMARY KEY,
	event_id varchar{255) NOT NULL,
	start_time SMALLDATETIME NOT NULL,
	end_time SMALLDATETIME NOT NULL,
	precipitation DECIMAL(2,4) NOT NULL,
	type_id INTEGER NOT NULL,
	severity_id INTEGER NOT NULL,
	station_id INTEGER NOT NULL,
	FOREIGN KEY (type_id) REFERENCES weather_type (id),
	FOREIGN KEY (severity_id) REFERENCES weather_severity (id),
	FOREIGN KEY (station_id) REFERENCES station (id)
);


