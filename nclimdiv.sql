CREATE TABLE STATES (
    st_fips CHAR(3) PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE COUNTIES (
    county_fips CHAR(3),
    name VARCHAR,
    state CHAR(3),
    PRIMARY KEY (county_fips, state),
    FOREIGN KEY (state) REFERENCES STATES(st_fips)
);


CREATE TABLE STATE_RECORDS (
    yr CHAR(4),
    mo CHAR(2),
    state CHAR(3),
    pcp DECIMAL(4, 2),
    cdd INTEGER(4),
    hdd INTEGER(4),
    tmax DECIMAL(4, 1),
    tmin DECIMAL(4, 1),
    tavg DECIMAL(4, 1),
    pdsi DECIMAL(4, 2),
    phdi DECIMAL(4, 2),
    pmdi DECIMAL(4, 2),
    zndx DECIMAL(4, 2),
    PRIMARY KEY (yr, mo, state),
    FOREIGN KEY (state) REFERENCES STATES(st_fips)
);


CREATE TABLE COUNTY_RECORDS (
    yr CHAR(4),
    mo CHAR(2),
    county CHAR(3),
    state CHAR(3),
    pcp DECIMAL(4, 2),
    tmax DECIMAL(4, 1),
    tmin DECIMAL(4, 1),
    tavg DECIMAL(4, 1),
    PRIMARY KEY (yr, mo, county, state),
    FOREIGN KEY (county) REFERENCES COUNTIES(county_fips)
);

DELETE FROM STATE_RECORDS;

SELECT COUNT(*) FROM STATE_RECORDS;