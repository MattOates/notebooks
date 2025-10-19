# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb==1.4.1",
#     "polars[pyarrow]==1.34.0",
#     "sqlglot==27.27.0",
# ]
# ///

import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    pp_df = mo.sql(
        f"""
        DROP TABLE IF EXISTS price_paid_this_month;

        -- Property type (D/S/T/F/O)
        DROP TYPE IF EXISTS property_type_enum;
        CREATE TYPE property_type_enum AS ENUM (
            'Detached',
            'Semi-Detached',
            'Terraced',
            'Flats/Maisonettes',
            'Other'
        );

        -- Old/New (Y/N)
        DROP TYPE IF EXISTS old_new_enum;
        CREATE TYPE old_new_enum AS ENUM (
            'New',
            'Established'
        );

        -- Duration (F/L/H/Other)
        DROP TYPE IF EXISTS duration_enum;
        CREATE TYPE duration_enum AS ENUM (
            'Freehold',
            'Leasehold',
            'Heritable',
            'Other'
        );

        -- PPD Category Type (A/B)
        DROP TYPE IF EXISTS ppd_category_enum;
        CREATE TYPE ppd_category_enum AS ENUM (
            'Standard',
            'Additional'
        );

        -- Record Status (A/C/D)
        DROP TYPE IF EXISTS record_status_enum;
        CREATE TYPE record_status_enum AS ENUM (
            'Addition',
            'Change',
            'Delete'
        );

        -- General Price Paid Schema
        CREATE TABLE IF NOT EXISTS price_paid_this_month (
            transaction_id      VARCHAR,
            price               BIGINT,
            date_of_transfer    DATE,
            postcode            VARCHAR,
            property_type       property_type_enum,
            old_new             old_new_enum,
            duration            duration_enum,
            paon                VARCHAR,
            saon                VARCHAR,
            street              VARCHAR,
            locality            VARCHAR,
            town_city           VARCHAR,
            district            VARCHAR,
            county              VARCHAR,
            ppd_category_type   ppd_category_enum,
            record_status       record_status_enum
        );

        -- Get just the latest month data from GovUK AWS
        INSERT INTO price_paid_this_month
        SELECT 
            transaction_id,
            price,
            date_of_transfer,
            postcode,
            CASE property_type
                WHEN 'D' THEN 'Detached'
                WHEN 'S' THEN 'Semi-Detached'
                WHEN 'T' THEN 'Terraced'
                WHEN 'F' THEN 'Flats/Maisonettes'
                WHEN 'O' THEN 'Other'
            END::property_type_enum AS property_type,
            CASE old_new
                WHEN 'Y' THEN 'New'
                WHEN 'N' THEN 'Established'
            END::old_new_enum AS old_new,
            CASE duration
                WHEN 'F' THEN 'Freehold'
                WHEN 'L' THEN 'Leasehold'
                WHEN 'H' THEN 'Heritable'
            END::duration_enum AS duration,
            paon,
            saon,
            street,
            locality,
            town_city,
            district,
            county,
            CASE ppd_category_type
                WHEN 'A' THEN 'Standard'
                WHEN 'B' THEN 'Additional'
            END::ppd_category_enum AS ppd_category_type,
            CASE record_status
                WHEN 'A' THEN 'Addition'
                WHEN 'C' THEN 'Change'
                WHEN 'D' THEN 'Delete'
            END::record_status_enum AS record_status
        FROM read_csv_auto('http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv',
            header = FALSE,
            columns = {{
                'transaction_id': 'VARCHAR',
                'price': 'BIGINT',
                'date_of_transfer': 'DATE',
                'postcode': 'VARCHAR',
                'property_type': 'VARCHAR',
                'old_new': 'VARCHAR',
                'duration': 'VARCHAR',
                'paon': 'VARCHAR',
                'saon': 'VARCHAR',
                'street': 'VARCHAR',
                'locality': 'VARCHAR',
                'town_city': 'VARCHAR',
                'district': 'VARCHAR',
                'county': 'VARCHAR',
                'ppd_category_type': 'VARCHAR',
                'record_status': 'VARCHAR'
            }}
        );
        SELECT * FROM price_paid_this_month;
        """
    )
    return


if __name__ == "__main__":
    app.run()
