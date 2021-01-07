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

declare
  job_name csv_exporter_jobs.cej_name%type := 'Move RDBMS Data';
BEGIN
  -- Make it easier to review the logs...
  
  delete from csv_exporter_log;
  commit;
  
  -- Create/re-create the job.
  
  CSV_EXPORTER.create_job(
    in_job_name      => job_name,
    in_oracle_dir    => 'LOADER_DIR',  /* Target Oracle "CREATE DIRECTORY" directory for CSV/SQL files */
    in_threads       => 4,             /* Should be less than physical cores */
    in_remote_host   => 'exporter.hana.sizingtool.us',  /* Target HANA host */
    in_remote_dir    => '/usr/sap/HXE/HDB90/loader',    /* Landing zone for CSV - ../exports added */
    in_remote_user   => 'hxeadm',      /* SSH/SCP user - no password required */
    in_hana_user     => 'csvadm',      /* HANA technical user doing the imports (**needs work**)*/
    in_hana_schema   => 'TARGET_SCHEMA /* Target schema in HANA */
  );
  
  -- Add all visible tables (SELECT privilege) from the SOURCE_SCHEMA schema.
  
  CSV_EXPORTER.add_schema(job_name, 'SOURCE_SCHEMA');
  
  -- Kick off the job.  Future runs for the same job/tables only require this call to run_job.
  CSV_EXPORTER.run_job(job_name);
END;
