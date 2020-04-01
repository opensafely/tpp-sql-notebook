"""
The aim of this module is to provide functions for defining patient-level
covariates (and later cohort extraction and case matching) in a way that, as
far as possible, captures the high-level intention and avoids the specific
details of how and where the data is stored.
"""
import datetime
import re

import pandas
import pyodbc


# These are all the public functions/variables that should be exposed from this module.
# Methods on `Covariate` instances are also public, but the constructor is not.
__all__ = [
    "PATIENT_ID_COL",
    "set_backend",
    "patients_with_these_medications",
    "patients_with_these_clinical_events",
    "patients_with_age_and_sex",
]

# This the name of the patient ID column as publically exposed by this module,
# not the internal name used by the database
PATIENT_ID_COL = "patient_id"

# Characters that are safe to interpolate into SQL (see
# `placeholders_and_params` below)
SAFE_CHARS_RE = re.compile(r"[a-zA-Z0-9_\.\-]+")

# Global to hold details of the current "backend". At present this just means
# the connecton to the TPP database but we aim to support other backends in
# future
backend = {}


class Covariate:
    """
    Represents a set of patients together with one or more values for those
    patients
    """

    def __init__(self, columns, sql, params):
        for column in columns:
            validate_column_name(column)
        self.columns = tuple(columns)
        self._sql = sql
        self._params = params
        self._df = None

    def inspect_sql(self):
        # Useful for debugging purposes; not all backends will support this as
        # not all will use SQL
        return self._sql

    def to_df(self):
        """
        Return the covariate as a Pandas dataframe
        """
        if self._df is None:
            self._df = pandas.read_sql_query(
                self._sql,
                get_sql_connection(),
                params=self._params,
                index_col=PATIENT_ID_COL,
            )
        return self._df

    def to_csv(self, filename):
        self.to_df().to_csv(filename)


def patients_with_these_medications(
    column_name, snomed_codes, min_date=None, max_date=None
):
    """
    Patients who have been prescribed at least one of this list of medications
    in the defined period
    """
    placeholders, params = placeholders_and_params(snomed_codes)
    date_condition, date_params = make_date_filter(
        "ConsultationDate", min_date, max_date
    )
    params.extend(date_params)
    sql = f"""
        SELECT
            DISTINCT med.Patient_ID AS {PATIENT_ID_COL},
            1 AS {column_name}
        FROM MedicationDictionary AS dict
        INNER JOIN MedicationIssue AS med
        ON dict.MultilexDrug_ID = med.MultilexDrug_ID
        WHERE dict.DMD_ID IN ({placeholders}) AND {date_condition}
        ORDER BY {PATIENT_ID_COL}
        """
    return Covariate([column_name], sql, params)


def patients_with_these_clinical_events(
    column_name, ctv3_codes, min_date=None, max_date=None
):
    """
    Patients who have had at least one of these clinical events in the defined
    period
    """
    placeholders, params = placeholders_and_params(ctv3_codes)
    date_condition, date_params = make_date_filter(
        "ConsultationDate", min_date, max_date
    )
    params.extend(date_params)
    sql = f"""
        SELECT
            DISTINCT Patient_ID AS {PATIENT_ID_COL},
            1 AS {column_name}
        FROM CodedEvent
        WHERE CTV3Code IN ({placeholders}) AND {date_condition}
        ORDER BY {PATIENT_ID_COL}
        """
    return Covariate([column_name], sql, params)


def patients_with_age_and_sex(
    reference_date, min_age=None, max_age=None, sex=None, patient_ids=None
):
    """
    Patients with their sex (M or F) and their age calculated as of the
    supplied reference date (which can be a YYYY-MM-DD string or "today")

    Patients can be filtered by age and sex and also restricted to a supplied
    list of patient IDs
    """
    reference_date, min_date, max_date = ages_to_dates(reference_date, min_age, max_age)
    params = [reference_date]
    conditions = []
    if min_date:
        conditions.append("DateOfBirth >= ?")
        params.append(min_date)
    if max_date:
        conditions.append("DateOfBirth <= ?")
        params.append(max_date)
    if sex is not None:
        conditions.append("sex = ?")
        params.append(sex)
    if patient_ids is not None:
        placeholders, patient_params = placeholders_and_params(patient_ids)
        conditions.append(f"Patient_ID IN ({placeholders})")
        params.extend(patient_params)
    sql = f"""
        SELECT
          Patient_ID AS {PATIENT_ID_COL},
          DATEDIFF(year, DateOfBirth, ?) AS age,
          Sex as sex
        FROM
          Patient
        WHERE
          {" AND ".join(conditions)}
        """
    return Covariate(["age", "sex"], sql, params)


def set_backend(
    name, hostname="localhost", port=1433, username=None, password=None, database=None
):
    """
    Sets the details of the backend in use. Currently only TPP is supported. A
    "dummy" backend returning random data might be the next candidate.
    """
    assert name == "tpp", "'tpp' is only supported backend currently"
    connection_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={hostname},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    backend["connection_str"] = connection_str


def get_sql_connection():
    if "connection_str" not in backend:
        raise RuntimeError(
            "You must configure a database connection using "
            "`set_backend` before attempting to run queries"
        )
    if "connection" not in backend:
        backend["connection"] = pyodbc.connect(backend["connection_str"])
    return backend["connection"]


def validate_column_name(column_name):
    if not re.match(r"[a-z0-9_]+", column_name):
        raise ValueError(
            f"Invalid column name '{column_name}', only lowercase "
            "alphanumeric and underscore allowed"
        )


def placeholders_and_params(values, as_ints=False):
    """
    Returns parameter placeholders for use in an SQL `IN` condition, together
    with a list of parameters to be used.

    Ideally the function would just be this:

      placeholders = ','.join("?" * len(values))
      params = values

    However, the pyodbc driver uses "prepared statements" under the hood and
    these have a maximum limit of 2100 parameters, which we exceed when using
    large codelists. (See: https://github.com/mkleehammer/pyodbc/issues/576).
    One way of working around this would be to use temporary tables, but for
    now we just manually interpolate the values into the SQL. Rather than
    attempt any escaping we simply apply a whitelist of known safe characters.
    As the codes we use come from quite a restricted character set this
    shouldn't be a problem. And if it is we'll just blow up with an error
    rather than do anything dangerous.
    """
    if as_ints:
        raise NotImplementedError("TODO")
    values = list(map(str, values))
    for value in values:
        if not SAFE_CHARS_RE.match(value):
            raise ValueError(f"Value contains disallowed characters: {value}")
    quoted_values = [f"'{value}'" for value in values]
    placeholders = ",".join(quoted_values)
    params = []
    return placeholders, params


def make_date_filter(column, min_date, max_date):
    if min_date is not None and max_date is not None:
        return f"{column} BETWEEN ? AND ?", [min_date, max_date]
    elif min_date is not None:
        return f"{column} >= ", [min_date]
    elif max_date is not None:
        return f"{column} <= ", [max_date]
    else:
        return "1=1", []


def ages_to_dates(now, min_age, max_age):
    if now == "today":
        now = datetime.date.today()
    now = datetime.date.fromisoformat(str(now))
    if now.month == 2 and now.day == 29:
        now = datetime.date(now.year, now.month, 28)
    min_date, max_date = None, None
    if min_age is not None:
        max_date = str(datetime.date(now.year - min_age, now.month, now.day))
    if max_age is not None:
        min_date = str(datetime.date(now.year - max_age, now.month, now.day))
    return str(now), min_date, max_date
