-- Basic Entity: hospital_data stores all the weekly updated information from the HHS and CMS data, like hospital beds count. 
-- hospital_info stores the more permanent data, like hospital name, address, and rating.
-- hospital_location stores the more permanent data, including fips, latitude and longtitude

-- from HHS data
-- id: a unique identifier for combination of hospital + date
-- hospital_id: a unique ID for each hospital, corresponds to hospital_pk
-- name: the name of the hospital facility
-- state: two letter state id 
-- address: street address of the hospital
-- city: city name the hospital is in
-- zip: five digit zip code of hospital
-- fips: a five digit unique identifier for countries
-- latitude: geocoded latitude of the hospital
-- longitude: geocoded longitude of the hospital
-- avalible_adult_beds: The total number of adult hospital beds available that week, it’s a 7 day average
-- avalible_pediatric_beds: The total number of pediatric beds available that week, it’s a 7 day average
-- occupied_adult_beds: The total number of adult hospital beds that are in use that week, it’s a 7 day average
-- occupied_pediatric_beds: The total number of pediatric beds that are in use that week, it’s a 7 day average
-- available_ICU_beds: The total number of ICU (intensive care unit) beds available that week, it’s a 7 day average
-- occupied_ICU_beds: The total number of ICU (intensive care unit) beds that are in use that week, it’s a 7 day average
-- COVID_beds_use:The number of patients hospitalized who have confirmed COVID, it’s a 7 day average
-- COVID_ICU_use: The number of adult ICU patients who have confirmed COVID, it’s a 7 day average

-- CMS data
-- hospital_type: The type of hospital
-- ownership: The type of ownership of the hospital (government, private, non-profit, etc.)
-- emergency_service: Whether the hospital provides emergency services
-- quality_rating: The hospital’s overall quality rating, updates multiple times a year

-- Misc
-- collection_date: the date when the data is collected


CREATE TABLE hospital_data (
	id SERIAL PRIMARY KEY,
	hospital_id TEXT NOT NULL,
	collection_date DATE CHECK(collection_date <= CURRENT_DATE) NOT NULL,
	avalible_adult_beds DECIMAL CHECK(avalible_adult_beds >= 0),
	avalible_pediatric_beds DECIMAL CHECK(avalible_pediatric_beds >= 0), 
	occupied_adult_beds DECIMAL CHECK(occupied_adult_beds >= 0),
	occupied_pediatric_beds DECIMAL CHECK(occupied_pediatric_beds >= 0),
	available_ICU_beds DECIMAL CHECK(available_ICU_beds >= 0),
	occupied_ICU_beds DECIMAL CHECK(occupied_ICU_beds >= 0),
	COVID_beds_use DECIMAL CHECK(COVID_beds_use >= 0),
	COVID_ICU_use DECIMAL CHECK(COVID_ICU_use >= 0),
	UNIQUE (hospital_id, collection_date)
);

CREATE TABLE hospital_info (
	hospital_id TEXT PRIMARY KEY NOT NULL,
	name TEXT NOT NULL,
	hospital_type TEXT NOT NULL,
	ownership TEXT NOT NULL, 
	collection_date DATE CHECK(collection_date <= CURRENT_DATE) NOT NULL,
	state CHAR(2),
	address TEXT,
	city TEXT,
	zip CHAR(5),
	emergency_service BOOLEAN,
	quality_rating INT CHECK(quality_rating <= 5 AND quality_rating > 0)
);

CREATE TABLE hospital_location (
	hospital_id TEXT PRIMARY KEY NOT NULL,
	collection_date DATE CHECK(collection_date <= CURRENT_DATE) NOT NULL,
	fips CHAR(5),
	latitude DECIMAL, 
	longitude DECIMAL
);
