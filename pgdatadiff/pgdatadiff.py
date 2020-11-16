import sys
import warnings
import time

from fabulous.color import bold, green, red
from halo import Halo
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import create_engine
from sqlalchemy.exc import NoSuchTableError, ProgrammingError, OperationalError
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.schema import MetaData, Table


def make_session(connection_string):
    engine = create_engine(connection_string, echo=False,
                           convert_unicode=True)
    Session = sessionmaker(bind=engine)
    return Session(), engine


class DBDiff(object):

    def __init__(self, firstdb, seconddb, chunk_size=100000, count_only=False):
        firstsession, firstengine = make_session(firstdb)
        secondsession, secondengine = make_session(seconddb)
        self.firstsession = firstsession
        self.firstengine = firstengine
        self.secondsession = secondsession
        self.secondengine = secondengine
        self.firstmeta = MetaData(bind=firstengine)
        self.secondmeta = MetaData(bind=secondengine)
        self.firstinspector = inspect(firstengine)
        self.secondinspector = inspect(secondengine)
        self.chunk_size = int(chunk_size)
        self.count_only = count_only

    def diff_table_data(self, tablename):
        try:
            if self.count_only is True:
                firsttable = Table(tablename, self.firstmeta, autoload=True)
                firstquery = self.firstsession.query(
                    firsttable)
                secondtable = Table(tablename, self.secondmeta, autoload=True)
                secondquery = self.secondsession.query(
                    secondtable)
                first_table_count = firstquery.count()
                if first_table_count != secondquery.count():
                    return False, f"counts are different" \
                        f" {first_table_count} != {secondquery.count()}"
                if first_table_count == 0:
                    return None, "tables are empty"
                return True, "Counts are the same"
        except NoSuchTableError:
            return False, "table is missing"

        pks = self.firstinspector.get_pk_constraint(tablename)[
                            'constrained_columns']
        if not pks:
            return None, "no primary key(s) on this table." \
                            " Comparison is not possible."

        next_offset_select_expr = ', '.join(
            ('last({pk})'.format(pk=pk) for pk in pks)
        )
        order_expr = ', '.join(pks)
        offset_expr = ' AND '.join('{pk} >= :{pk}'.format(pk=pk) for pk in pks)

        SQL_TEMPLATE_HASH = f"""
        SELECT
            md5(array_agg(md5((t.*)::varchar))::varchar) as hash,
            {next_offset_select_expr}
        FROM
            (
                SELECT * from {tablename}
                WHERE NOT :has_offset OR ({offset_expr})
                ORDER BY {order_expr}
                LIMIT {self.chunk_size}
            ) t;
        """

        done = False
        position = 0
        offsets = {pk: None for pk in pks}

        while not done:
            offsets['has_offset'] = position > 0
            firstresult = retry(lambda: self.firstsession.execute(
                SQL_TEMPLATE_HASH,
                offsets))
            secondresult = retry(lambda: self.secondsession.execute(
                SQL_TEMPLATE_HASH,
                offsets))

            if firstresult.rowcount != secondresult.rowcount:
                return False, f"row count mismatch at row {position}; " \
                              f"offsets: {offsets}"

            (firsthash, *firstpks) = firstresult.fetchone()
            (secondhash, *secondpks) = secondresult.fetchone()

            if firsthash != secondhash:
                return False, f"data hash are different at row {position}; " \
                              f"offsets: {offsets}"

            if firstpks != secondpks:
                return False, f"data pks are different  at row {position};" \
                              f"offsets: {offsets}"

            position += self.chunk_size
            for idx, pk in enumerate(pks):
                offsets[pk] = firstpks[idx]
        return True, "data is identical."

    def get_all_sequences(self):
        GET_SEQUENCES_SQL = """SELECT c.relname FROM
        pg_class c WHERE c.relkind = 'S';"""
        return [x[0] for x in
                self.firstsession.execute(GET_SEQUENCES_SQL).fetchall()]

    def diff_sequence(self, seq_name):
        GET_SEQUENCES_VALUE_SQL = f"SELECT last_value FROM {seq_name};"

        try:
            firstvalue = retry(
                lambda: self.firstsession.execute(GET_SEQUENCES_VALUE_SQL)
                .fetchone()[0]
            )
            secondvalue = retry(
                lambda: self.secondsession.execute(GET_SEQUENCES_VALUE_SQL)
                .fetchone()[0]
            )
        except ProgrammingError:
            self.firstsession.rollback()
            self.secondsession.rollback()

            return False, "sequence doesnt exist in second database."
        if firstvalue < secondvalue:
            return None, f"first sequence is less than" \
                         f" the second({firstvalue} vs {secondvalue})."
        if firstvalue > secondvalue:
            return False, f"first sequence is greater than" \
                          f" the second({firstvalue} vs {secondvalue})."
        return True, f"sequences are identical- ({firstvalue})."

    def diff_all_sequences(self):
        print(bold(red('Starting sequence analysis.')))
        sequences = sorted(self.get_all_sequences())
        failures = 0
        for sequence in sequences:
            status_update = StatusUpdate(
                f"Analysing sequence {sequence}. "
                f"[{sequences.index(sequence) + 1}/{len(sequences)}]"
            )
            result, message = self.diff_sequence(sequence)
            status_update.complete(result, f"{sequence} - {message}")
            if result is False:
                failures += 1
        print(bold(green('Sequence analysis complete.')))
        if failures > 0:
            return 1
        return 0

    def diff_all_table_data(self):
        failures = 0

        self.create_aggregate_functions()

        print(bold(red('Starting table analysis.')))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)
            tables = sorted(
                self.firstinspector.get_table_names(schema="public"))
            for table in tables:
                status_update = StatusUpdate(
                    f"Analysing table {table}. "
                    f"[{tables.index(table) + 1}/{len(tables)}]"
                )
                result, message = self.diff_table_data(table)
                status_update.complete(result, f"{table} - {message}")
                if result is False:
                    failures += 1
        print(bold(green('Table analysis complete.')))
        if failures > 0:
            return 1
        return 0

    def create_aggregate_functions(self):
        print('creating aggregate functions')
        stmt = """
        CREATE OR REPLACE FUNCTION public.last_agg ( anyelement, anyelement )
        RETURNS anyelement LANGUAGE sql IMMUTABLE STRICT AS $$
                SELECT $2;
        $$;

        -- And then wrap an aggregate around it
        CREATE AGGREGATE public.last (
                sfunc    = public.last_agg,
                basetype = anyelement,
                stype    = anyelement
        );
        """
        self.firstsession.execute(stmt)
        self.secondsession.execute(stmt)
        self.firstsession.commit()
        self.secondsession.commit()


class StatusUpdate(object):
    def __init__(self, title):
        if sys.stdout.isatty():
            self.spinner = Halo(title, spinner='dots')
            self.spinner.start()
        else:
            print(title)

    def complete(self, success, message):
        if self.spinner:
            if success is True:
                self.spinner.succeed(message)
            elif success is False:
                self.spinner.fail(message)
            else:
                self.spinner.warn(message)
            self.spinner.stop()
        else:
            if success is True:
                print("success: ", message)
            elif success is False:
                print("failed: ", message)
            else:
                print("warning: ", message)


def retry(fn):
    i = 0
    max_tries = 3
    base_timeout = 1
    while True:
        try:
            return fn()
        except Exception as ex:
            if (not isinstance(ex, DatabaseError) and
                    not isinstance(ex, OperationalError)):
                raise
            print('operational error running query:', ex)
            if i < max_tries:
                delay = 2**i * base_timeout
                print(
                    f'Attempt {i+1} of {max_tries}, retrying in {delay} secs.'
                )
                time.sleep(delay)
            else:
                raise
            i += 1
