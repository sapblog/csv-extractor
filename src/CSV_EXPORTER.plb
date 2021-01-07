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

create or replace package body csv_exporter as
    -- Forward reference to the dump rountine - not techically
    -- necessary, but I prefer to have this function last in
    -- the package body.
       
    function dump_table_to_csv(in_table     in varchar2,
                              in_directory in varchar2,
                              in_filename  in varchar2,
                              hana_columns  out varchar2) return integer;

    -- All DBMS_SCHEDULER jobs create will have this prefix.
    job_prefix varchar2(30) := 'CSV_EXPORTER_';

    -- The following templates are used to build the script
    -- run on the HANA server after the files are transferred.

    -- Setup the import statement.  The "#" variables will be
    -- replaced with the proper input file, table and log file
    -- values.
    
    import_template varchar2(2000) :=
'
import from csv file ''#f'' into #t 
   with batch 100000
   column list in first row
   field delimited by '','' optionally enclosed by ''"''
   error log ''#l'';
';

    -- The following is an anonymous HANA SQLScript block to
    -- figure out if the table exists in HANA - if not, the
    -- create statement is executed.  Schema, table and column
    -- values are substituted per table.
    
    create_table_template varchar2(2000) :=
'
do
begin
    declare table_count integer;

    select count(1) into table_count
      from tables
     where schema_name = ''#s''
       and table_name = ''#t'';
      
	if table_count = 0 then
   		execute immediate ''
CREATE COLUMN TABLE #h (
#c);
'';
    end if;
end;
';
    
    -- Utility function to write entries to the log.
    procedure log_entry(in_scope varchar2,
                        in_level varchar2,
                        in_msg   varchar2) is
        pragma autonomous_transaction;
    begin
        insert into csv_exporter_log (cel_scope, cel_level, cel_message)
                               values (in_scope, in_level, in_msg);

        commit;
    end;

    -- Utility function to determine seconds between timestamps, i.e., elapsed time.
    function timestamp_diff(a timestamp, b timestamp) return number is 
    begin
      return extract (day    from (a-b))*24*60*60 +
             extract (hour   from (a-b))*60*60+
             extract (minute from (a-b))*60+
             extract (second from (a-b));
    end;

    -- Remove a job by deleting the job record and the
    -- table entries associated with the job - both of these
    -- table were populated via the "create_job" procedure.
    
    procedure drop_job(in_job_name varchar2) is
        pragma autonomous_transaction;
    begin
        log_entry('drop_job', 'fine', 'Deleting job: ' || in_job_name);

        delete from csv_exporter_jobs where cej_name = in_job_name;
          
        log_entry('drop_job', 'fine', 'Removed ' || SQL%ROWCOUNT || ' job.');

        delete from csv_exporter_tables where cet_name = in_job_name;

        log_entry('drop_job', 'fine', 'Removed ' || SQL%ROWCOUNT || ' table(s).');

        commit;
        
        log_entry('drop_job', 'fine', 'Exiting drop_job');
    end;

    procedure create_job(in_job_name       varchar2,
                         in_oracle_dir     varchar2,
                         in_threads        integer default 1,
                         in_remote_host    varchar2,
                         in_remote_dir     varchar2,
                         in_remote_user    varchar2,
                         in_hana_user      varchar2,
                         in_hana_schema    varchar2) is
        pragma autonomous_transaction;

        table_cursor   sys_refcursor;

        table_sequence integer := 0;
        cur_thread     integer := 0;
    begin
        log_entry('create_job', 'fine', 'Creating job "' || in_job_name || '"');
        
        -- Do a little bit of house keeping - drop any existing job
        -- with the same name.
        
        drop_job(in_job_name);

        -- Create the job using all the input parameters.
        
        insert into csv_exporter_jobs (cej_name, 
                                       cej_oracle_dir, cej_threads,
                                       cej_remote_host, cej_remote_dir,  cej_remote_user,
                                       cej_hana_user, cej_hana_schema)
                            values (in_job_name, in_oracle_dir, in_threads,
                                    in_remote_host, in_remote_dir, in_remote_user,
                                    in_hana_user, in_hana_schema);

        commit;
        
        log_entry('create_job', 'Info', 'Job ' || in_job_name || ' created.');
    end;
    
    -- Add all the tables visible in a schema to a job.
    
    procedure add_schema(in_job_name varchar2, in_schema varchar2) is
        no_tables_found_exception exception;
        pragma exception_init(no_tables_found_exception, -20001);
    begin
        log_entry('add_schema', 'fine', 'Adding schema "' || in_schema || '" to job "' || in_job_name || '"');
        
        -- Capture all the visible tables - if CSV_EXPORTER doesn't have
        -- at least "GRANT SELECT" on a table, it will not be seen here.
        
        -- NOTE: exclude our tables in case we are installed in a
        --       schema with table we want to export.
        
        insert into csv_exporter_tables (cet_name, cet_schema, cet_table)
            select in_job_name, t.owner, t.table_name
              from all_tables t
             where t.owner = in_schema
             and t.table_name not like 'CSV_EXPORTER%';

        if sql%rowcount = 0 then
            log_entry('add_schema', 'warn', 'No tables found for schema "' || in_schema || '" for job "' || in_job_name || '"!');
            
            raise_application_error(-20001, 'No tables found for schema "' || in_schema || '" for job "' || in_job_name || '"!');
        else
            log_entry('add_schema', 'info', 'Added ' || sql%rowcount || ' tables from schema "' || in_schema || '" to job "' || in_job_name || '".');
        end if;
        
        commit;
        
        log_entry('add_schema', 'fine', 'Exiting add_schema');
    end;

    -- Add a single table to an existing job.
    
    procedure add_table(in_job_name varchar2, in_schema varchar2, in_table varchar2) is
        no_table_found_exception exception;
        pragma exception_init(no_table_found_exception, -20001);
    begin
        log_entry('add_table', 'fine', 'Adding table "' || in_schema || '"."' || in_table || '" to job "' || in_job_name || '"');
        
        -- Capture all the visible tables - if CSV_EXPORTER doesn't have
        -- at least "GRANT SELECT" on a table, it will not be seen here.
        
        insert into csv_exporter_tables (cet_name, cet_schema, cet_table)
            select in_job_name, t.owner, t.table_name
              from all_tables t
             where t.owner = in_schema
             and t.table_name = in_table;

        if sql%rowcount = 0 then
            log_entry('add_table', 'error', 'Adding table "' || in_schema || '"."' || in_table || '" to job "' || in_job_name || '"');
            
            raise_application_error(-20001, 'Table "' || in_schema || '"."' || in_table || '" not found for job "' || in_job_name || '"');
        else
            log_entry('add_table', 'error', 'Added table "' || in_schema || '"."' || in_table || '" to job "' || in_job_name || '"');
        end if;
        
        commit;
        
        log_entry('add_table', 'info', 'Exiting add_table');
    end;
    
    -- This procedure launches a job into the scheduler.  This routine is
    -- called for each thread at startup and again as each thread finishes
    -- to kick off the next table.

    -- NOTE: All scheduler jobs are set to "auto_drop" to keep the enviroment clean.
    
    procedure submit_job(in_job_name varchar2, in_thread integer) is
        job_name varchar2(200) := dbms_scheduler.generate_job_name(job_prefix);
    begin
        log_entry('submit_job', 'fine', 'Scheduled thread: ' || in_job_name || ' (thread ' || in_thread || ')');
        
        -- Before starting a new table job, make sure the we are still
        -- running the job, i.e., stop_job has not been invoked.
        -- If we stop running here, this thread dies (see stop_job).

        for dummy_rec in (select cej_status
                            from csv_exporter_jobs
                           where cej_name = in_job_name
                             and cej_status not in ('Running', 'Complete')) loop
            -- If the job has been stopped, exit immediately.
            
            log_entry('run_thread', 'warn', 'STOP COMMAND DETECTED - stopping thread:' || in_thread);

            return;
        end loop;

        /* Create a job to go for the next available table. The job will
           determine if there are any table left to process.
           
           We don't detect the status of tables here to make sure we
           stay asyc.
        */
        
        dbms_scheduler.create_job(
            job_name             => job_name,
            job_type             => 'STORED_PROCEDURE',
            job_action           => 'CSV_EXPORTER.EXPORT_TABLE',
            number_of_arguments  => 2,
            enabled              => false,
            auto_drop            => true);    --<<< AUTO DROP = TRUE

        dbms_scheduler.set_job_argument_value(
            job_name           => job_name,
            argument_position  => 1,  
            argument_value     => in_job_name);  

        dbms_scheduler.set_job_argument_value(
            job_name           => job_name,
            argument_position  => 2,  
            argument_value     => in_thread);  
            
        dbms_scheduler.enable(job_name);  -- Start the job.
    exception when others then
        log_entry('submit_job', 'error', 'Exception submitting job: ' || in_job_name || ' - SQLCODE: ' || SQLCODE);
        log_entry('submit_job', 'error', 'ERROR_STACK: ' || DBMS_UTILITY.FORMAT_ERROR_STACK);
        log_entry('submit_job', 'error', 'ERROR_BACKTRACE: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

        raise;
    end;
    
    -- This procedure submits an async job to retrieve the log file 
    -- from the operating system and insert it into the log table.
    
    -- Again, we are staying very async.
    
    procedure submit_script_log(in_job_name varchar2, in_table_guid varchar2) is
        pragma autonomous_transaction;
        job_name varchar2(200) := dbms_scheduler.generate_job_name(job_prefix);
    begin
        log_entry('submit_script_log', 'fine', 'Scheduled for job: ' || in_job_name || ' - table guild ' || in_table_guid);

        /* Go for the next available table. */
        
        dbms_scheduler.create_job(
            job_name             => job_name,
            job_type             => 'STORED_PROCEDURE',
            job_action           => 'CSV_EXPORTER.GET_SCRIPT_LOG',
            start_date           => current_timestamp + interval '10' second,
            number_of_arguments  => 2,
            enabled              => false,
            auto_drop            => true);    --<<< AUTO DROP = TRUE

        dbms_scheduler.set_job_argument_value(
            job_name           => job_name,
            argument_position  => 1,  
            argument_value     => in_job_name);  

        dbms_scheduler.set_job_argument_value(
            job_name           => job_name,
            argument_position  => 2,  
            argument_value     => in_table_guid);  
            
        dbms_scheduler.enable(job_name);  -- Start the job.
        
        commit;
        
        log_entry('submit_script_log', 'fine', 'Done: ' || in_job_name || ' (thread ' || in_table_guid || ')');
    exception when others then
        log_entry('submit_script_log', 'error', 'Exception submitting job: ' || in_job_name || ' - SQLCODE: ' || SQLCODE);
        log_entry('submit_script_log', 'error', 'ERROR_STACK: ' || DBMS_UTILITY.FORMAT_ERROR_STACK);
        log_entry('submit_script_log', 'error', 'ERROR_BACKTRACE: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

        raise;
    end;
    
    -- Retrieve the log file of the zip-n-ship from the OS directory
    -- and store it in the log table.
    
    procedure get_script_log(in_job_name varchar2, in_table_guid varchar2) is
        pragma autonomous_transaction;
        bfile_handle BFILE;
        clob_content CLOB;
        
        src_offset   INTEGER := 1;
        dest_offset  INTEGER := 1;
        bfile_csid   NUMBER  := 1;
        lang_context INTEGER := 0;
        warning      INTEGER := 0;
        
        log_filename varchar2(400) := in_table_guid || '.log';
    begin
        log_entry('get_script_log', 'fine', 'Entering with guid "' || in_table_guid || '" for job "' || in_job_name || '".');
        
        -- Only do something if there is a CSV_EXPORTER_TABLES row with
        -- the guid argument.
        
        for tab_rec in (select cet_table, cej_oracle_dir
                          from csv_exporter_tables
                               join csv_exporter_jobs on cet_name = cej_name
                         where cet_guid = in_table_guid) loop
            log_entry('get_script_log', 'fine', 'Table for guid: ' || tab_rec.cet_table);
            
            -- Check to see if the job is still running - the log cannot
            -- be complete if the job has not finished.
            
            for job_rec in (select * 
                              from user_scheduler_job_run_details
                             where job_name = in_job_name) loop
                log_entry('get_script_log', 'fine', 'Job status: ' || job_rec.status);
                
                if job_rec.status IN ('SUCCEEDED', 'FAILED') then
                    log_entry('get_script_log', 'info', 'Zip/ship no longer running: ' || job_rec.status || '.');
                    
                    -- Start a clob on the row of this table so we can
                    -- load the log contents.
                    
                    update csv_exporter.csv_exporter_tables
                       set cet_script_log = empty_clob()
                     where cet_guid = in_table_guid
                    returning cet_script_log into clob_content;

                    -- Open the log file from the directory of this job.
                    bfile_handle := BFILENAME('HHS', log_filename);
                    DBMS_LOB.fileopen(bfile_handle, DBMS_LOB.file_readonly);
                    
                    -- Pull the contents of the log file into the CLOB.
                    DBMS_LOB.loadclobfromfile (
                        dest_lob     => clob_content,
                        src_bfile    => bfile_handle,
                        amount       => DBMS_LOB.lobmaxsize,
                        dest_offset  => dest_offset,
                        src_offset   => src_offset,
                        bfile_csid   => bfile_csid,
                        lang_context => lang_context,
                        warning      => warning);
                    
                    -- We are done with the log file.
                    DBMS_LOB.fileclose(bfile_handle);

                    COMMIT;

                    -- Now remove the log file
                    utl_file.fremove(tab_rec.cej_oracle_dir, log_filename);
                    log_entry('get_script_log', 'fine', 'Removed log file: ' || log_filename);
                    
                    log_entry('get_script_log', 'fine', 'Log file has been commited.');
                    
                    -- Be returning at this point, we are stopping this job checking process.
                    return;
                else
                    log_entry('get_script_log', 'info', 'Zip/ship job has not finished - resubmit.');
                    submit_script_log(in_job_name, in_table_guid);
                    
                    -- We are done here - the scheduler will resubmit in a few seconds.
                    return;
                end if;
            end loop;
        end loop;
        
        -- We should never get here!  This would indicate that we are looking
        -- for a table guid that does not exist.
        
        log_entry('get_script_log', 'fatal', 'Table GUID not found: ' || in_table_guid);
    end;
    
    -- This procedure IS THE ENTRY POINT that starts a job and
    -- launches as many scheduler jobs as threads specified 
    -- when the job was created.
    
    procedure run_job(in_job_name varchar2) is
        pragma autonomous_transaction;

        job_not_found_exception exception;
        pragma exception_init(job_not_found_exception, -20001);

        thread_indx integer := -1;
        valid_job   boolean := false;
    begin
        log_entry('run_job', 'fine', 'Entering with job name: ' || in_job_name);

        -- The following loop does not run if the job name does not exist.
        
        for job_rec in (select cej_threads
                          from csv_exporter_jobs 
                         where cej_name = in_job_name) loop
            -- Set the status to running so the threads processor can be halted if necessary

            update csv_exporter_jobs 
               set cej_status = 'Running',
                   cej_start_ts = systimestamp,
                   cej_elapsed = 0
             where cej_name = in_job_name;

            -- The statistics for the tables should have been updated before
            -- launching because the table are processed largest to smallest
            -- based on the current statistics.
            
            update csv_exporter_tables t
               set cet_status = 'Pending',
                   cet_estimated_rows = (select sum(NVL(s.num_rows, 0))
                                           from all_tab_statistics s
                                          where s.owner = t.cet_schema
                                            and s.table_name = t.cet_table),
                   cet_elapsed        = null,
                   cet_script_log     = null
             where cet_name = in_job_name;

            -- Make sure each table has an identifier (GUID) we use these to
            -- manage the artifiacts (sql/csv/log) created by process.
            
            update csv_exporter_tables t
               set cet_guid = sys_guid()
             where cet_name = in_job_name
               and cet_guid is null;
               
            commit;
            
            -- Start the threads - each thread independantly manages
            -- the processing of pending tables.
            
            for thread_indx in 1 .. job_rec.cej_threads loop
                submit_job(in_job_name, thread_indx);
            end loop;
            
            -- Make a note that we did find and start a job!
            valid_job := true;
        end loop;

        if not valid_job then
            log_entry('run_job', 'warn', 'Job ' || in_job_name || ' was not found!');
            
            raise_application_error(-20001, 'Job "' || in_job_name || '" does not exist.');
        end if;
        
        log_entry('run_job', 'fine', 'Exiting run_job : ' || in_job_name);
    end;

    procedure stop_job(in_job_name varchar2) is
        pragma autonomous_transaction;
    begin
        -- Simply set the status of the job to stopped.  Eventually, all the
        -- threads will detect this and shut themselves down.
        
        update csv_exporter_jobs
           set cej_status = 'Stopped'
         where cej_name = in_job_name;

        commit;
    end;

    -- This is the primary entry point for exporting a table to CSV.
    
    procedure export_table(in_job_name varchar2, in_thread integer) is
        pragma autonomous_transaction;
        
        table_rowid     rowid := null;
        
        cursor table_cursor(in_rowid rowid)  is
            select cej_oracle_dir,
                   cej_remote_host, cej_remote_dir, cej_remote_user,
                   cej_hana_user, cej_hana_schema,
                   cet_schema, cet_table, cet_guid
              from csv_exporter_jobs j
                   join csv_exporter_tables t
                     on cej_name = cet_name
             where t.rowid = in_rowid
               and cej_name = cet_name;

        table_rec      table_cursor%rowtype;
                             
        table_count     integer;
        row_count       integer;

        job_status      varchar2(200);
        job_name        varchar2(200);
        local_dir_name  varchar2(200);

        csv_filename    varchar2(200);
        sql_filename    varchar2(200);
        log_filename    varchar2(200);
        fq_table        varchar2(200);
        
        hana_tablename  varchar2(2000);
        hana_filename   varchar2(2000);
        hana_logfile    varchar2(2000);
        import_stmt     varchar2(2000);
        file_handle     utl_file.file_type;
        
        create_table_stmt varchar2(32000);
        table_columns     varchar2(32000);

        start_ts        timestamp;
        end_ts          timestamp;
        elapsed_seconds integer;
    begin
        -- Get a lock so we can identify the next table without getting
        -- into a race condition with other threads.  Other jobs, if 
        -- they get here at the same time, will wait for us to get the
        -- next table and then continue when we release the lock.
        
        lock table csv_exporter_tables in exclusive mode;
        
        -- Find the next pending table to process.
        
        for tab_rec in (select rowid from csv_exporter_tables
                         where cet_name = in_job_name
                           and cet_status = 'Pending'
                         order by cet_estimated_rows desc) loop
            table_rowid := tab_rec.rowid;
            
            -- Change the status so no other thread will try
            -- to export this table.
            
            update csv_exporter_tables
               set cet_status = 'CSV',
                   cet_thread = in_thread
             where rowid = tab_rec.rowid;
            
            exit;  -- Exit the loop - we only need one table.
        end loop;
        
        commit;  -- Release the lock and apply the update if we found a table.

        -- If we didn't find a table than we are no longer processing, i.e.,
        -- the job has finished all the tables - update the job statistics.
        
        -- Updating the statistics occurs as each thread stops running so 
        -- we will have the complete elapsed time after the last thread
        -- finishes.
        
        if table_rowid is null then
            select cej_start_ts, systimestamp 
              into start_ts, end_ts
              from csv_exporter_jobs
             where cej_name = in_job_name;
    
            elapsed_seconds := timestamp_diff(end_ts, start_ts);
            
            update csv_exporter_jobs m
               set cej_end_ts = end_ts,
                   cej_elapsed = elapsed_seconds,
                   cej_status = 'Complete',
                   cej_table_count = (select count(1)
                                      from csv_exporter_tables t
                                     where t.cet_name = m.cej_name),
                   cej_row_count = (select sum(cet_row_count)
                                      from csv_exporter_tables t
                                     where t.cet_name = m.cej_name)
             where cej_name = in_job_name;
    
            commit;
            
            return;  -- This thread is done - shut it down.
        end if;
        
        -- Get all the details we need to process this table.
        
        open table_cursor(table_rowid);
        fetch table_cursor into table_rec;
                
        -- Go ahead and process the table.

        log_entry('run_thread', 'info', 'Starting table "' || table_rec.cet_schema || '"."' || table_rec.cet_table || '" in thread ' || in_thread || '.');
        
        start_ts := systimestamp;  -- Capture the start time for performance metrics.

        csv_filename := table_rec.cet_guid || '.csv';
        sql_filename := table_rec.cet_guid || '.sql';
        log_filename := table_rec.cet_guid || '.log';
        
        fq_table := '"' || table_rec.cet_schema || '"."' || table_rec.cet_table || '"';

        -- This call dumps the table to disk.
        row_count := dump_table_to_csv(fq_table, 
                                       table_rec.cej_oracle_dir,
                                       csv_filename,
                                       table_columns);
        
        end_ts := systimestamp;       
        elapsed_seconds := timestamp_diff(end_ts, start_ts);  -- How long did the dump take?
        
        -- Now write the HANA sql file to create the table and import the data ON THE HANA server.
        
        hana_tablename := '"' || table_rec.cej_hana_schema || '"."' || table_rec.cet_table || '"';
        hana_filename := table_rec.cej_remote_dir || '/exports/' || csv_filename;
        hana_logfile  := '/tmp/' || log_filename;
        
        create_table_stmt := replace(create_table_template, '#s', table_rec.cej_hana_schema);
        create_table_stmt := replace(create_table_stmt, '#t', table_rec.cet_table);
        create_table_stmt := replace(create_table_stmt, '#h', hana_tablename);
        create_table_stmt := replace(create_table_stmt, '#c', trim(table_columns));

        import_stmt := replace(import_template, '#f', hana_filename);
        import_stmt := replace(import_stmt, '#t', hana_tablename);
        import_stmt := replace(import_stmt, '#l', hana_logfile);
        
        file_handle := utl_file.fopen(table_rec.cej_oracle_dir, sql_filename, 'w');
       
        utl_file.put_line(file_handle, create_table_stmt);
        utl_file.put_line(file_handle, 'truncate table ' || hana_tablename || ';' || CHR(10));
        utl_file.put_line(file_handle, import_stmt);
        
        utl_file.fclose(file_handle);
        
        -- This table is done - update the status.
        
        update csv_exporter_tables
           set cet_status    = 'Exported',
               cet_row_count = row_count,
               cet_start_ts  = start_ts,
               cet_end_ts    = end_ts,
               cet_elapsed   = elapsed_seconds
         where cet_name  = in_job_name
           and cet_table = table_rec.cet_table;

        commit;

        -- Now zip and ship the file using another DBMS_SCHEDULER job.  By making
        -- this async to the dump, we free up the thread to move to the next table
        -- without waiting on the zip-n-ship process.

        log_entry('run_thread', 'info', 'Submitting zip/ship for ' || hana_tablename);

        select directory_path into local_dir_name
          from all_directories
         where directory_name = table_rec.cej_oracle_dir;

        job_name := dbms_scheduler.generate_job_name(job_prefix);

        dbms_scheduler.create_job(
            job_name => job_name,
            job_type => 'EXECUTABLE',
            job_action => csv_exporter_shell,
            number_of_arguments => 6,  /* Includes the script name */
            enabled => false,
            auto_drop => true);

        dbms_scheduler.set_job_argument_value(
            job_name => job_name, 
            argument_position => 1, 
            argument_value => csv_exporter_script);

        dbms_scheduler.set_job_argument_value(
            job_name => job_name, 
            argument_position => 2,
            argument_value => local_dir_name);

        dbms_scheduler.set_job_argument_value(
            job_name => job_name, 
            argument_position => 3,
            argument_value => table_rec.cet_guid);

        dbms_scheduler.set_job_argument_value(
            job_name => job_name, 
            argument_position => 4,
            argument_value => table_rec.cej_remote_host);

        dbms_scheduler.set_job_argument_value(
            job_name => job_name, 
            argument_position => 5,
            argument_value => table_rec.cej_remote_user);

        dbms_scheduler.set_job_argument_value(
            job_name => job_name, 
            argument_position => 6,
            argument_value => table_rec.cej_remote_dir);

        dbms_scheduler.enable(job_name);

        commit;
        
        -- Start the log file watching job so we can collect the script output.
        
        submit_script_log(job_name, table_rec.cet_guid);
        
        -- Keep this thread of jobs alive - go look for another
        -- table to export.
        
        submit_job(in_job_name, in_thread);
        
        log_entry('run_thread', 'info', 'Exiting export file ' || table_rec.cet_table);
    exception when others then
        log_entry('export_table', 'error', 'Exception exporting table - SQLCODE: ' || SQLCODE);
        log_entry('export_table', 'error', 'ERROR_STACK: ' || DBMS_UTILITY.FORMAT_ERROR_STACK);
        log_entry('export_table', 'error', 'ERROR_BACKTRACE: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

        raise;
    end;
    
    -- This is the workhorse of this solution!
    
    function dump_table_to_csv(in_table     in varchar2,
                               in_directory in varchar2,
                               in_filename  in varchar2,
                               hana_columns  out varchar2) return integer is
        file_handle      utl_file.file_type;
        cursor_handle    integer default dbms_sql.open_cursor;
        
        query_status         integer;
        table_query          varchar2(1000) default 'select * from ' || in_table;
        column_count         number;
        column_descriptions  dbms_sql.desc_tab3;
        supported_column     boolean;
        row_count            integer := 0;

        comma_separator      varchar2(1) default '';
        csv_buffer           varchar2(32000) := '';
        hana_column_def      varchar(200);

        column_value         varchar2(32000);
        current_row          varchar2(32000);
        
        -- All the possible column data types.

        v_varchar          varchar2(32767);
        v_long             long;
        v_number           number;
        
        v_clob             clob;
        v_blob             blob;
        
        v_date             date;
        v_timestamp        timestamp;
        v_timestamp_w_tz   timestamp with time zone;
        v_timestamp_w_ltz  timestamp with local time zone;

        v_binary_float     binary_float;
        v_binary_double    binary_double;
        v_raw              raw(32767);
        v_long_raw         long raw;

        v_rowid            rowid;

        v_buffer           raw(16100);
        v_start            pls_integer;
        v_buffer_size      pls_integer := 16100;
        v_output           varchar2(32767);       
        
        line_feed          char(1) := CHR(10);
        line_feed_enc      char(2) := '\n';
        
        quote              char(1) := '"';
        quote_enc          char(2) := '\"';
        
        -- Utility function to return a comment on the Oracle format.  This
        -- is included in the HANA script so we can see how Oracle defined
        -- this column - mostly debugging.
        
        function getDesc(in_indx integer) return varchar2 is
            outString varchar2(2000) := ' /*';
        begin
            outString := outString || ' T: ' || column_descriptions(in_indx).col_type;
            outString := outString || ' L: ' || column_descriptions(in_indx).col_max_len;
            outString := outString || ' P: ' || column_descriptions(in_indx).col_precision;
            outString := outString || ' P: ' || column_descriptions(in_indx).col_scale;
            outString := outString || ' N: ' || column_descriptions(in_indx).col_type_name;
            
            return outString || ' */';
        end;
        
        -- Utility function to clean up newline and quotes so the CSV file
        -- works on the HANA side.
        
        -- NOTE: This clean-up is specific to how HANA handles CSV escape characters.
        
        procedure encode(varchar_val in out varchar2) is
        begin
            varchar_val := replace(varchar_val, line_feed, line_feed_enc);
            varchar_val := replace(varchar_val, quote, quote_enc);
        end;

        -- Setup the dynamic SQL for this table and the HANA translation
        -- for each possbile Oracle data type - unhandled types will
        -- be skipped both in the DDL and the export. For each supported
        -- column type, we build both the DBMS_SQL column definitions 
        -- and the HANA DDL.
        
        -- NOTE: unsupported types are skipped, all other data is exported.
        
        -- This routine depends heavily on procedure scoped variables.
        
        procedure initialize_columns is
        begin
            for i in 1 .. column_count loop
                -- Assume this will be a supported column type - most will be.
                supported_column := true;
                
                case column_descriptions(i).col_type
                when dbms_sql.Varchar2_Type then
                    dbms_sql.define_column( cursor_handle, i, v_varchar, 4000);
                    
                    hana_column_def := 'NVARCHAR(' || column_descriptions(i).col_max_len || ')';
                    
                when dbms_sql.Number_Type then
                    dbms_sql.define_column( cursor_handle, i, v_number);
                    
                    if column_descriptions(i).col_scale = 0 then
                        hana_column_def := 'INTEGER';
                    else
                        hana_column_def := 'NUMBER(' || column_descriptions(i).col_precision || ',' || column_descriptions(i).col_scale || ')';
                    end if;
                    
                when dbms_sql.Long_Type then
                    dbms_sql.define_column( cursor_handle, i, v_long);
                    
                    hana_column_def := 'CLOB';
                    
                when dbms_sql.Rowid_Type then
                    dbms_sql.define_column( cursor_handle, i, v_rowid);
                    
                    hana_column_def := 'NUMBER(' || column_descriptions(i).col_max_len || ')';
                    
                when dbms_sql.Date_Type then
                    dbms_sql.define_column( cursor_handle, i, v_date);
    
                    hana_column_def := 'DATE';
                    
                when dbms_sql.Raw_Type then
                    hana_column_def := 'VARBINARY';
                    dbms_sql.define_column( cursor_handle, i, v_raw );
                    
                when dbms_sql.Long_Raw_Type then
                    hana_column_def := 'BLOB';
                    dbms_sql.define_column( cursor_handle, i, v_long_raw );
    
                when dbms_sql.Char_Type then
                    hana_column_def := 'CHAR(' || column_descriptions(i).col_max_len || ')';
                    dbms_sql.define_column( cursor_handle, i, v_varchar, 4000);
                    
                when dbms_sql.Clob_Type then
                    hana_column_def := 'CLOB';
                    dbms_sql.define_column( cursor_handle, i, v_clob);
    
                when dbms_sql.Blob_Type then
                    dbms_sql.define_column( cursor_handle, i, v_blob);
    
                    hana_column_def := 'BLOB';
                    
                when dbms_sql.Bfile_Type then
                    dbms_sql.define_column( cursor_handle, i, v_blob);
                    
                    hana_column_def := 'BLOB';
                    
                when dbms_sql.Timestamp_Type then
                    dbms_sql.define_column( cursor_handle, i, v_timestamp);
    
                    hana_column_def := 'TIMESTAMP';
    
                when dbms_sql.Timestamp_With_TZ_Type then
                    dbms_sql.define_column( cursor_handle, i, v_timestamp);
    
                    hana_column_def := 'TIMESTAMP';
    
                when dbms_sql.Timestamp_With_Local_TZ_type then
                    dbms_sql.define_column( cursor_handle, i, v_timestamp);
    
                    hana_column_def := 'TIMESTAMP';
                    
                else
                    supported_column := false;
                end case;
    
                -- If this column is supported then add it to the CSV header
                -- and define a column to be returned with dynamic sql.
                
                if supported_column then
                    csv_buffer := csv_buffer || comma_separator || column_descriptions(i).col_name;
                    
                    hana_columns := hana_columns || comma_separator ||
                                    rpad('"' || column_descriptions(i).col_name || '"', 40) ||
                                    hana_column_def || getDesc(i)|| chr(10);
                               
                    comma_separator := ',';
                end if;
            end loop;
    
            csv_buffer := csv_buffer || chr(10);
        end;
        
    begin
        log_entry('dump_table_to_csv', 'info', 'Starting CSV dump for ' || in_table || ' to file ' || in_filename);
        
        -- Open the CSV file in write+binary mode.  We are using a buffer
        -- size of 32K and we flush when we get close to filling the buffer.
        
        file_handle := utl_file.fopen(in_directory, in_filename, 'wb', 32767);

        -- Setup the dynamic SQL.
        
        dbms_sql.parse( cursor_handle, table_query, dbms_sql.native );
        dbms_sql.describe_columns3( cursor_handle, column_count, column_descriptions );

        hana_columns := '';  -- Initialze

        initialize_columns;  -- Figure out the columns
        
        -- Execute the dynamic query to get the rows for the table.

        query_status := dbms_sql.execute(cursor_handle);  -- QUERY IT!

        while ( dbms_sql.fetch_rows(cursor_handle) > 0 ) loop
            row_count := row_count +  1;

            -- Build the CSV for the current row - pay attention
            -- to quotes, dates and timestamps.  The conversions
            -- are specific to HANA default formats.

            current_row := '';
            comma_separator := '';

            for i in 1 .. column_count loop
                supported_column := true;
                
                case column_descriptions(i).col_type
                when dbms_sql.varchar2_type then
                    dbms_sql.column_value( cursor_handle, i, v_varchar);
                    encode(v_varchar);
                    
                    column_value := '"' || v_varchar || '"';

                when dbms_sql.char_type then
                    dbms_sql.column_value( cursor_handle, i, v_varchar);
                    encode(v_varchar);
                    
                    column_value := '"' || v_varchar || '"';

                when dbms_sql.date_type then
                    dbms_sql.column_value( cursor_handle, i, v_date);
                    column_value := to_char(v_date, 'YYYY-MM-DD');

                when dbms_sql.Number_Type then
                    dbms_sql.column_value( cursor_handle, i, v_number);
                    column_value := v_number;

                when dbms_sql.Long_Type then
                    dbms_sql.column_value( cursor_handle, i, v_long);
                    column_value := v_long;

                when dbms_sql.Binary_Float_Type then
                    dbms_sql.column_value( cursor_handle, i, v_binary_float);
                    column_value := v_binary_float;

                when dbms_sql.Binary_Double_Type then
                    dbms_sql.column_value( cursor_handle, i, v_binary_double);
                    column_value := v_binary_double;

                when dbms_sql.Timestamp_Type then
                    dbms_sql.column_value( cursor_handle, i, v_timestamp);
                    column_value := TO_CHAR(v_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF7');

                when dbms_sql.Timestamp_With_TZ_Type then
                    dbms_sql.column_value( cursor_handle, i, v_timestamp);
                    column_value := TO_CHAR(v_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF7');

                when dbms_sql.Timestamp_With_Local_TZ_type then
                    dbms_sql.column_value( cursor_handle, i, v_timestamp);
                    column_value := TO_CHAR(v_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF7');

                when dbms_sql.Rowid_Type then
                    dbms_sql.column_value( cursor_handle, i, v_rowid);
                    column_value := v_rowid;

                when dbms_sql.Clob_Type then
                    dbms_sql.column_value( cursor_handle, i, v_clob);
                    encode(v_clob);
                    
                    column_value := '"' || v_clob || '"';

                when dbms_sql.Blob_Type then
                    dbms_sql.column_value( cursor_handle, i, v_blob);
                    
                    -- Blobs force the writing of the CSV buffer so the blob
                    -- can be pumped directly to the output file.
                    
                    utl_file.put_raw( file_handle, UTL_RAW.CAST_TO_RAW(csv_buffer));
                    utl_file.fflush(file_handle);
                    
                    utl_file.put_raw( file_handle, UTL_RAW.CAST_TO_RAW(comma_separator));
                    comma_separator := ',';
                    
                    utl_file.put_raw(file_handle, UTL_RAW.CAST_TO_RAW('"'));
                    utl_file.fflush(file_handle);
                    
                    v_start := 1;
                    
                    for i in 1..ceil(dbms_lob.getlength(v_blob) / v_buffer_size) loop
                       v_buffer := dbms_lob.substr(v_blob, v_buffer_size, v_start);
                       
                       utl_file.put_raw(file_handle, UTL_RAW.CAST_TO_RAW(rawtohex(v_buffer)));
                       utl_file.fflush(file_handle);
                       
                       v_start := v_start + v_buffer_size;
                    end loop;
                    
                    utl_file.put_raw(file_handle, UTL_RAW.CAST_TO_RAW('"'));
                    utl_file.fflush(file_handle);
                    
                    csv_buffer := '';
                    
                    -- Since we have handled the BLOB already, skip the normal
                    -- column addition.
                    
                    supported_column := false;
                else
                    supported_column := false;
                end case;

                if supported_column then
                    current_row := current_row || comma_separator || column_value;
    
                    comma_separator := ',';
                end if;
            end loop;

            current_row := current_row || chr(10);

            if length(csv_buffer) + length(current_row) > 32000 then
                utl_file.put_raw( file_handle, UTL_RAW.CAST_TO_RAW(csv_buffer));
                utl_file.fflush(file_handle);

                csv_buffer := '';
            end if;

            csv_buffer := csv_buffer || current_row;
        end loop;

        if length(csv_buffer) > 0 then 
            utl_file.put_raw(file_handle, UTL_RAW.CAST_TO_RAW(csv_buffer));
        end if;

        dbms_sql.close_cursor(cursor_handle);
        
        utl_file.fflush(file_handle);
        utl_file.fclose( file_handle );

        return row_count;
    exception
    when others then
        log_entry('dump_table_to_csv', 'error', 'Exception procssing file ' || in_table || ' - SQLCODE: ' || SQLCODE);
        log_entry('dump_table_to_csv', 'error', 'ERROR_STACK: ' || DBMS_UTILITY.FORMAT_ERROR_STACK);
        log_entry('dump_table_to_csv', 'error', 'ERROR_BACKTRACE: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        
        raise;
    end;
end csv_exporter;