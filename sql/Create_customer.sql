CREATE OR REPLACE TABLE CUSTOMER (
    CUSTOMER_ID       INT AUTOINCREMENT PRIMARY KEY,  -- Unique ID for each customer
    CUSTOMER_NAME     STRING NOT NULL,               -- Customer's name
    EMAIL            STRING UNIQUE,                 -- Email must be unique
    PHONE_NUMBER     STRING,                         -- Contact number
    ADDRESS          STRING,                         -- Full address
    CITY             STRING,                         -- City name
    STATE            STRING,                         -- State name
    ZIP_CODE         STRING,                         -- Postal code
    COUNTRY          STRING DEFAULT 'USA',          -- Default country as USA
    CREATED_AT       TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation date
);