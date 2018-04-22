# -*- coding: utf-8 -*-
import ctd
import luigi
from luigi import six

# defined in Batch entry point Task (shared among all tasks)
batch_name = "n.a."  # concurrent batch jobs should be launched in separate process


def postgres_target(target_table, update_id):
    # TODO:  re-implement my own PostgresTarget to do :
    #  1) avoid exposing password
    #  2) leverage stating.load_audit as a "marker" table
    return luigi.postgres.PostgresTarget(
            host        =ctd.config.DATABASE['host'],
            database    =ctd.config.DATABASE['database'],
            user        =ctd.config.DATABASE['user'],
            password    =ctd.config.DATABASE['password'],
            port        =ctd.config.DATABASE['port'],
            table       =target_table,
            update_id   =update_id)

    
class BasePostgresTask(luigi.Task):
    """
    Designed for subclass Task that updates (dml) DB target 
    
    Subclass must provide target table (self.table) & implement SQL logic in exec_sql().
    """

    def output(self):
        return postgres_target(self.table, self.task_id)

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()

        # decorate with audit-log stuff
        self.audit_id, self.run_dts = ctd.elt.insert_auditing(batch_name, self.task_id)
        self.rowscount = self.exec_sql(cursor, self.audit_id)
        cursor.close()

        # mark as complete in same transaction (checkpoint)
        self.output().touch(connection)
        # commit and clean up
        connection.commit()
        connection.close()

    def on_success(self):
        ctd.elt.update_auditing(self.audit_id, ctd.elt.EltStepStatus.COMPLETED,
                                run_dts=self.run_dts, rows=self.rowscount)

    def on_failure(self, exception):
        ctd.elt.update_auditing(self.audit_id, ctd.elt.EltStepStatus.FAILED,
                                run_dts=self.run_dts, output=str(exception))

    def exec_sql(self, cursor, audit_id):
        raise NotImplementedError


class BaseBulkLoadTask(luigi.postgres.CopyToTable):
    """
    Designed for subclass Task designed to bulkload file to DB target
    
    Subclass must provide target table, columns (and separator) & implement requires() logic.

    Should create a simple/clean implementation to avoid luigi's issue
    """
    # (cannot use postgre_target() as attributes set as abstractproperty in rdbms.CopyToTable)
    host = ctd.config.DATABASE['host']
    database = ctd.config.DATABASE['database']
    user = ctd.config.DATABASE['user']
    password = ctd.config.DATABASE['password']
    port = ctd.config.DATABASE['port']

    clear_table_before = False
    # default separator
    column_separator = '|'
    # added to manage col headers
    input_has_headers = False

    def output(self):
        """
        TODO: Luigi issues: forgot to include port
        """
        return luigi.postgres.PostgresTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
            table=self.table,
            update_id=self.update_id()
        )

    def requires(self):
        raise NotImplementedError

    def __init__(self, *args, **kwargs):
        super(BaseBulkLoadTask, self).__init__(*args, **kwargs)
        self.run_dts = None
        self.audit_id = None
        self.rowscount = None

    def init_copy(self, connection):
        if self.clear_table_before:
            connection.cursor().execute('truncate table %s;' % self.table)

    def rows(self):
        """
        TODO: signal issue: split by tab instead of self.column_seperator!
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split(self.column_separator)

    def run(self):

        # if file not empty, read 1st line (header)
        header = None
        with self.input().open('r') as fobj:
            try:
                header = fobj.next()
            # avoid executing downstream Task for empty file
            except StopIteration as e:
                raise ImportError("File empty, task %s is stopped" % self.task_id)

        if self.input_has_headers and header:
            self.columns = header.strip('\n').split(self.column_separator)

        # decorate with audit-log stuff
        self.audit_id, self.run_dts = ctd.elt.insert_auditing(batch_name, self.task_id)
        super(BaseBulkLoadTask, self).run()

        
    # overriding copy() needed to handle file with col headers (and return rowscount)
    # TODO: suggest to add to luigi:  accept headers and populate columns based on these..
    def copy(self, cursor, file):
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))

        if self.input_has_headers:
            self.rowscount = self.copy_expert(file, cursor)
        else:
            cursor.copy_from(file, self.table, null=r'\\N', sep=self.column_separator, columns=column_names)
            self.rowscount = cursor.rowcount

    def copy_expert(self, infile, cursor):
        """
        Execute copy_expert
        :return rowcount impacted
        """
        # Now use default NULL (empty and \N) since luigi generated tmp file is ok (following the ovewrite of rows()
        sql = \
            """
            copy %s( %s )
            from STDIN with csv HEADER DELIMITER '%s';""" \
            % (self.table, ",".join(self.columns), self.column_separator)

        cursor.copy_expert(sql, infile, size=8192)
        return cursor.rowcount

    def on_success(self):
        if self.audit_id:
            ctd.elt.update_auditing(self.audit_id, ctd.elt.EltStepStatus.COMPLETED,
                                    run_dts=self.run_dts, rows=self.rowscount)

    def on_failure(self, exception):
        if self.audit_id:
            ctd.elt.update_auditing(self.audit_id, ctd.elt.EltStepStatus.FAILED,
                                    run_dts=self.run_dts, output=str(exception))

                                    