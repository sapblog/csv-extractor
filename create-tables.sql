/*
##########################################################################
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# For more information, please refer to <http://unlicense.org/>
##########################################################################
*/

drop table csv_exporter_log;
drop table csv_exporter_jobs;
drop table csv_exporter_tables;

-- Logging table so jobs can be monitored and errors are captured.
create table csv_exporter_log (
    cel_timestamp    timestamp default systimestamp,
    cel_scope        varchar2(200),
    cel_level        varchar2(200),
    cel_message      clob);

-- A job is defines the from and to details on the Oracle and HANA platforms.
-- Last run statistics are also recorded.
create table csv_exporter_jobs (
    cej_name            varchar2(200) primary key,
    cej_oracle_dir      varchar2(200),
    cej_threads         integer,
    cej_remote_host     varchar2(400),
    cej_remote_dir      varchar2(400),
    cej_remote_user     varchar2(400),
    cej_hana_user       varchar2(400),
    cej_hana_schema     varchar2(400),
    cej_status          varchar2(400),
    cej_start_ts        timestamp,
    cej_end_ts          timestamp,
    cej_elapsed         integer,
    cej_table_count     integer,
    cej_row_count       integer);

-- For each job, an explicit list of tables is maintained.  At the
-- start of a job, all the statuses are set to Pending.  When there
-- are no more Pending tables, the job stops.
create table csv_exporter_tables (
    cet_name            varchar2(200),
    cet_schema          varchar2(200),
    cet_table           varchar2(200),
    cet_status          varchar2(200),
    cet_thread          integer,
    cet_estimated_rows  integer,
    cet_row_count       integer,
    cet_start_ts        timestamp,
    cet_end_ts          timestamp,
    cet_elapsed         integer,
    cet_guid            varchar2(32),
    cet_script_log      clob);  