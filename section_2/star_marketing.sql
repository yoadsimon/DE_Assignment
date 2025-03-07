CREATE TABLE dim_ad_hierarchy (
    ad_hierarchy_key SERIAL PRIMARY KEY,
    account_id       TEXT NOT NULL,
    account_name     TEXT NOT NULL,
    sub_account_id   TEXT NOT NULL,
    sub_account_name TEXT NOT NULL,
    portfolio_id     TEXT NOT NULL,
    portfolio_name   TEXT NOT NULL,
    campaign_id      TEXT NOT NULL,
    campaign_name    TEXT NOT NULL,
    ad_group_id      TEXT NOT NULL,
    ad_group_name    TEXT NOT NULL,
    ad_id            TEXT NOT NULL,
    ad_name          TEXT NOT NULL,
    valid_from       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to         TIMESTAMP NOT NULL DEFAULT '9999-12-31 00:00:00',
    CONSTRAINT uq_dim_ad_hierarchy UNIQUE (ad_id, valid_from)
);

CREATE TABLE dim_date (
    date_key    INT PRIMARY KEY,
    full_date   TEXT NOT NULL,
    year        INT NOT NULL,
    quarter     INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    day_of_week INT NOT NULL
);

CREATE TABLE dim_device (
    device_key  SERIAL PRIMARY KEY,
    device_name TEXT NOT NULL
);

CREATE TABLE dim_gveo (
    geo_key  SERIAL PRIMARY KEY,
    geo_name TEXT NOT NULL
);

CREATE TABLE fact_ad_performance (
    fact_key         SERIAL PRIMARY KEY,
    ad_hierarchy_key INT  NOT NULL,
    click_date_key   INT,
    view_date_key    INT,
    device_key       INT  NOT NULL,
    geo_key          INT  NOT NULL,
    cost            NUMERIC(10,2) NOT NULL,
    impressions     INT          NOT NULL,
    clicks          INT          NOT NULL,
    bid             NUMERIC(10,2) NOT NULL,
    label           TEXT,
    load_timestamp  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ad_hierarchy_key) REFERENCES dim_ad_hierarchy(ad_hierarchy_key),
    FOREIGN KEY (click_date_key)   REFERENCES dim_date(date_key),
    FOREIGN KEY (view_date_key)    REFERENCES dim_date(date_key),
    FOREIGN KEY (device_key)       REFERENCES dim_device(device_key),
    FOREIGN KEY (geo_key)          REFERENCES dim_geo(geo_key)
);

COMMIT;