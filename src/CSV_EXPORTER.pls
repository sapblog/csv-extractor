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

create or replace package  csv_exporter as
    -- This is the fully qualified name of the exporter script
    -- used to run the zip-n-ship process.  The Oracle directory
    -- must exist pointing to this location.
    csv_exporter_script varchar2(2000) := '/home/oracle/csv_exporter.sh';
    csv_exporter_shell  varchar2(2000) := '/bin/bash';
    
    -- Create a new job with the Oracle/HANA connectivity information.
    procedure create_job(in_job_name       varchar2,
                         in_oracle_dir     varchar2,
                         in_threads        integer default 1,
                         in_remote_host    varchar2,
                         in_remote_dir     varchar2,
                         in_remote_user    varchar2,
                         in_hana_user      varchar2,
                         in_hana_schema    varchar2);

    -- Utility function to add all the visible tables in a schema
    -- to a job.
    procedure add_schema(in_job_name   varchar2,
                         in_schema     varchar2);
                      
    -- Utility function to add a single table to a job - a job can
    -- pull from multiple schemas.
    procedure add_table(in_job_name    varchar2,
                        in_schema      varchar2,
                        in_table       varchar2);

    -- Start a job running.                        
    procedure run_job(in_job_name varchar2);
    
    -- Cancel a job - the job stops when all threads
    -- complete the operation (table dump) in progress.
    procedure stop_job(in_job_name varchar2);
    
    -- Remove a job.
    procedure drop_job(in_job_name varchar2);

    -- Private methods used by DBMS_SCHEDULER - do not call directly
    procedure export_table(in_job_name varchar2, in_thread integer);
    procedure get_script_log(in_job_name varchar2, in_table_guid varchar2);
end csv_exporter;