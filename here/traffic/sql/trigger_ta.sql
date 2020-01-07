CREATE OR REPLACE FUNCTION here.ta_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.pct_50_corr := CASE WHEN NEW.mean >= 30 AND NEW.mean < 110 THEN NEW.pct_50 * (1 - 0.051 * sqrt(least(1.0*(NEW.tx::DATE - '2018-03-01'::DATE)/('2019-01-01'::DATE - '2018-03-01'::DATE), 1.0)))
				          ELSE NEW.pct_50
				          END;
    NEW.mean_corr := CASE WHEN NEW.mean >= 30 AND NEW.mean < 110 THEN NEW.mean * (1 - 0.051 * sqrt(least(1.0*(NEW.tx::DATE - '2018-03-01'::DATE)/('2019-01-01'::DATE - '2018-03-01'::DATE), 1.0)))
				          ELSE NEW.mean
				          END;

	IF (NEW.tx >= DATE '2020-12-01' AND NEW.tx < DATE '2020-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202012 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-11-01' AND NEW.tx < DATE '2020-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202011 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-10-01' AND NEW.tx < DATE '2020-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202010 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-09-01' AND NEW.tx < DATE '2020-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202009 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-08-01' AND NEW.tx < DATE '2020-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202008 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-07-01' AND NEW.tx < DATE '2020-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202007 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-06-01' AND NEW.tx < DATE '2020-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202006 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-05-01' AND NEW.tx < DATE '2020-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202005 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-04-01' AND NEW.tx < DATE '2020-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202004 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-03-01' AND NEW.tx < DATE '2020-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202003 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-02-01' AND NEW.tx < DATE '2020-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202002 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2020-01-01' AND NEW.tx < DATE '2020-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_202001 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-12-01' AND NEW.tx < DATE '2019-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201912 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-11-01' AND NEW.tx < DATE '2019-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201911 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-10-01' AND NEW.tx < DATE '2019-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201910 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-09-01' AND NEW.tx < DATE '2019-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201909 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-08-01' AND NEW.tx < DATE '2019-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201908 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-07-01' AND NEW.tx < DATE '2019-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201907 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-06-01' AND NEW.tx < DATE '2019-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201906 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-05-01' AND NEW.tx < DATE '2019-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201905 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-04-01' AND NEW.tx < DATE '2019-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201904 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-03-01' AND NEW.tx < DATE '2019-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201903 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-02-01' AND NEW.tx < DATE '2019-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201902 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2019-01-01' AND NEW.tx < DATE '2019-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201901 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-12-01' AND NEW.tx < DATE '2018-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201812 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-11-01' AND NEW.tx < DATE '2018-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201811 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-10-01' AND NEW.tx < DATE '2018-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201810 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-09-01' AND NEW.tx < DATE '2018-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201809 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-08-01' AND NEW.tx < DATE '2018-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201808 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-07-01' AND NEW.tx < DATE '2018-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201807 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-06-01' AND NEW.tx < DATE '2018-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201806 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-05-01' AND NEW.tx < DATE '2018-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201805 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-04-01' AND NEW.tx < DATE '2018-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201804 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-03-01' AND NEW.tx < DATE '2018-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201803 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-02-01' AND NEW.tx < DATE '2018-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201802 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2018-01-01' AND NEW.tx < DATE '2018-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201801 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-12-01' AND NEW.tx < DATE '2017-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201712 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-11-01' AND NEW.tx < DATE '2017-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201711 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-10-01' AND NEW.tx < DATE '2017-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201710 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-09-01' AND NEW.tx < DATE '2017-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201709 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-08-01' AND NEW.tx < DATE '2017-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201708 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-07-01' AND NEW.tx < DATE '2017-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201707 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-06-01' AND NEW.tx < DATE '2017-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201706 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-05-01' AND NEW.tx < DATE '2017-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201705 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-04-01' AND NEW.tx < DATE '2017-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201704 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-03-01' AND NEW.tx < DATE '2017-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201703 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-02-01' AND NEW.tx < DATE '2017-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201702 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2017-01-01' AND NEW.tx < DATE '2017-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201701 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-12-01' AND NEW.tx < DATE '2016-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201612 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-11-01' AND NEW.tx < DATE '2016-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201611 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-10-01' AND NEW.tx < DATE '2016-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201610 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-09-01' AND NEW.tx < DATE '2016-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201609 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-08-01' AND NEW.tx < DATE '2016-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201608 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-07-01' AND NEW.tx < DATE '2016-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201607 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-06-01' AND NEW.tx < DATE '2016-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201606 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-05-01' AND NEW.tx < DATE '2016-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201605 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-04-01' AND NEW.tx < DATE '2016-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201604 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-03-01' AND NEW.tx < DATE '2016-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201603 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-02-01' AND NEW.tx < DATE '2016-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201602 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2016-01-01' AND NEW.tx < DATE '2016-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201601 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-12-01' AND NEW.tx < DATE '2015-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201512 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-11-01' AND NEW.tx < DATE '2015-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201511 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-10-01' AND NEW.tx < DATE '2015-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201510 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-09-01' AND NEW.tx < DATE '2015-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201509 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-08-01' AND NEW.tx < DATE '2015-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201508 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-07-01' AND NEW.tx < DATE '2015-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201507 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-06-01' AND NEW.tx < DATE '2015-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201506 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-05-01' AND NEW.tx < DATE '2015-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201505 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-04-01' AND NEW.tx < DATE '2015-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201504 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-03-01' AND NEW.tx < DATE '2015-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201503 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-02-01' AND NEW.tx < DATE '2015-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201502 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2015-01-01' AND NEW.tx < DATE '2015-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201501 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-12-01' AND NEW.tx < DATE '2014-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201412 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-11-01' AND NEW.tx < DATE '2014-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201411 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-10-01' AND NEW.tx < DATE '2014-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201410 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-09-01' AND NEW.tx < DATE '2014-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201409 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-08-01' AND NEW.tx < DATE '2014-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201408 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-07-01' AND NEW.tx < DATE '2014-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201407 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-06-01' AND NEW.tx < DATE '2014-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201406 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-05-01' AND NEW.tx < DATE '2014-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201405 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-04-01' AND NEW.tx < DATE '2014-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201404 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-03-01' AND NEW.tx < DATE '2014-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201403 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-02-01' AND NEW.tx < DATE '2014-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201402 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2014-01-01' AND NEW.tx < DATE '2014-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201401 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-12-01' AND NEW.tx < DATE '2013-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201312 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-11-01' AND NEW.tx < DATE '2013-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201311 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-10-01' AND NEW.tx < DATE '2013-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201310 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-09-01' AND NEW.tx < DATE '2013-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201309 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-08-01' AND NEW.tx < DATE '2013-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201308 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-07-01' AND NEW.tx < DATE '2013-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201307 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-06-01' AND NEW.tx < DATE '2013-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201306 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-05-01' AND NEW.tx < DATE '2013-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201305 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-04-01' AND NEW.tx < DATE '2013-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201304 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-03-01' AND NEW.tx < DATE '2013-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201303 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-02-01' AND NEW.tx < DATE '2013-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201302 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2013-01-01' AND NEW.tx < DATE '2013-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201301 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-12-01' AND NEW.tx < DATE '2012-12-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201212 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-11-01' AND NEW.tx < DATE '2012-11-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201211 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-10-01' AND NEW.tx < DATE '2012-10-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201210 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-09-01' AND NEW.tx < DATE '2012-09-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201209 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-08-01' AND NEW.tx < DATE '2012-08-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201208 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-07-01' AND NEW.tx < DATE '2012-07-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201207 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-06-01' AND NEW.tx < DATE '2012-06-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201206 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-05-01' AND NEW.tx < DATE '2012-05-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201205 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-04-01' AND NEW.tx < DATE '2012-04-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201204 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-03-01' AND NEW.tx < DATE '2012-03-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201203 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-02-01' AND NEW.tx < DATE '2012-02-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201202 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSIF (NEW.tx >= DATE '2012-01-01' AND NEW.tx < DATE '2012-01-01' +INTERVAL '1 month') THEN
 INSERT INTO here.ta_201201 VALUES (NEW.*)ON CONFLICT DO NOTHING;
ELSE 
	RAISE EXCEPTION 'tx out of range.  Fix the ta_insert_trigger() function!';
END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION here.ta_insert_trigger() OWNER TO here_admins;
	   
DROP TRIGGER IF EXISTS insert_trigger ON here.ta;
CREATE TRIGGER insert_trigger
    BEFORE INSERT
    ON here.ta
    FOR EACH ROW
    EXECUTE PROCEDURE here.ta_insert_trigger();