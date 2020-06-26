CREATE TABLE wys.stationary_summary (
    api_id integer,
    mon date,
    pct_05 INT,
    pct_10 INT,
    pct_15 INT,
    pct_20 INT,
    pct_25 INT,
    pct_30 INT,
    pct_35 INT,
    pct_40 INT,
    pct_45 INT,
    pct_50 INT,
    pct_55 INT,
    pct_60 INT,
    pct_65 INT,
    pct_70 INT,
    pct_75 INT,
    pct_80 INT,
    pct_85 INT,
    pct_90 INT,
    pct_95 INT,
    spd_00 INT,
    spd_05 INT,
    spd_10 INT,
    spd_15 INT,
    spd_20 INT,
    spd_25 INT,
    spd_30 INT,
    spd_35 INT,
    spd_40 INT,
    spd_45 INT,
    spd_50 INT,
    spd_55 INT,
    spd_60 INT,
    spd_65 INT,
    spd_70 INT,
    spd_75 INT,
    spd_80 INT,
    spd_85 INT,
    spd_90 INT,
    spd_95 INT,
    spd_100_and_above INT,
    volume INT
);

GRANT SELECT, INSERT ON TABLE wys.stationary_summary TO wys_bot;
GRANT SELECT ON TABLE wys.stationary_summary TO bdit_humans;

ALTER TABLE wys.stationary_summary ADD UNIQUE(sign_id, mon);