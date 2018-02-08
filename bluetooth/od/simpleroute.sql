﻿with route as (with ones as (select startend_path.datetime_bin, startend_path.userid, s_co_ba, e_co_un from alouis2.startend_path
where s_co_ba = 1 and e_co_un = 1)


select ones.datetime_bin, ones.userid, s_co_ba, e_co_un, du_ba, du_sp, du_un, path_total from ones


INNER JOIN others_path ON ones.userid = others_path.userid and ones.datetime_bin = others_path.datetime_bin

where path_total= 5)

select datetime_bin, userid, s_co_ba, e_co_un, du_ba, du_sp, du_un, path_total, startpoint_name, endpoint_name  from route, bluetooth.observations

where observations.user_id = route.userid and observations.measured_timestamp = route.datetime_bin;
















