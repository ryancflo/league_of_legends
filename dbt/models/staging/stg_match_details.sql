
CREATE OR REPLACE TABLE STAGING_MATCH_DETAILS_STAGE
(
    json_data variant
);

COPY INTO "STAGING_MATCH_DETAILS_STAGE"
FROM @MY_AZURE_MATCH_DETAILS_STAGE
file_format = MATCH_DETAILS;