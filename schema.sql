-- Basic Entity: hospital_data stores all the weekly updated information from the HHS and CMS data, like hospital beds count. 
-- hospital_info stores the more permanent data, like hospital name and address.

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
-- week: the number of week since the database starts tracking (1,2,3..)


CREATE TABLE hospital_data (
	id SERIAL PRIMARY KEY,
	hospital_id TEXT NOT NULL,
	collection_date DATE CHECK(collection_date <= CURRENT_DATE) NOT NULL,
	week INT CHECK(week > 0) NOT NULL,
	avalible_adult_beds INT CHECK(avalible_adult_beds >= 0),
	avalible_pediatric_beds INT CHECK(avalible_pediatric_beds >= 0), 
	occupied_adult_beds INT CHECK(occupied_adult_beds >= 0),
	occupied_pediatric_beds INT CHECK(occupied_pediatric_beds >= 0),
	available_ICU_beds INT CHECK(available_ICU_beds >= 0),
	occupied_ICU_beds INT CHECK(occupied_ICU_beds >= 0),
	COVID_beds_use INT CHECK(COVID_beds_use >= 0),
	COVID_ICU_use INT CHECK(COVID_ICU_use >= 0), 
	quality_rating INT CHECK(quality_rating <= 5 AND quality_rating > 0),
	CHECK(avalible_adult_beds >= occupied_adult_beds), 
	CHECK(avalible_pediatric_beds >= occupied_pediatric_beds), 
	CHECK(available_ICU_beds >= occupied_ICU_beds), 
	CHECK(available_ICU_beds >= COVID_ICU_use),
	CHECK(avalible_adult_beds + occupied_pediatric_beds >= COVID_beds_use)
);

CREATE TABLE hospital_info (
	hospital_id TEXT PRIMARY KEY NOT NULL,
	name TEXT NOT NULL,
	hospital_type TEXT NOT NULL,
	ownership TEXT NOT NULL, 
	state CHAR(2),
	address TEXT,
	city TEXT,
	zip CHAR(5),
	fips CHAR(5),
	latitude DECIMAL, 
	longitude DECIMAL,
	emergency_service BOOLEAN
);

