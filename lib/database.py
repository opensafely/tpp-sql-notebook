from contextlib import contextmanager
import datetime
import pyodbc
import pandas as pd
import os


@contextmanager
def closing_connection():
    server = f"{os.environ['DB_HOST']},1433"
    database = os.environ["DB_NAME"]
    username = "SA"
    password = os.environ["DB_PASS"]
    dsn = (
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + server
        + ";DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    )
    cnxn = pyodbc.connect(dsn)
    yield cnxn
    cnxn.close()


def sql_to_df(sql, params=[]):
    with closing_connection() as cnxn:
        df = pd.read_sql(sql, cnxn, params=params)
    return df


def codes_to_sql_where(col_name, code_list):
    where = ""
    i = 0
    for code in code_list:
        if i == 0:
            where = where + f"{col_name} = ?"
        else:
            where = where + f" OR {col_name} = ?"
        i += 1
    return (col_name, where, code_list)


def and_where_clauses(where_clauses):
    all_sql = []
    all_params = []
    for where_clause, params in where_clauses:
        all_sql.append(where_clause)
        all_params.extend(params)
    return "(" + " AND ".join(all_sql) + ")", all_params


def patients_with_age_condition(direction, age):
    allowed_directions = [">", "<", ">=", "<="]
    assert (
        direction in allowed_directions
    ), f"direction must be one of {allowed_directions}"
    today = datetime.date.today().strftime("%Y-%m-%d")
    return sql_to_df(
        f"""
        SELECT
          Patient_ID, DATEDIFF(year, DateOfBirth, ?) AS age
        FROM
          Patient
        WHERE
          DATEDIFF(year, DateOfBirth, ?) {direction} ?""",
        params=[today, today, age],
    )


def patients_with_codes_in_consecutive_quarters(
    clinical_code_list=[], medicine_code_list=[], quarters=3, strict_codes=False
):
    """Returns SQL that generates a table of patients who have ever had a clinical code
    """
    assert medicine_code_list, "This function requires at least one medicine code"
    where_clause = coded_event_join = ""
    if medicine_code_list:
        where_clause += f"AND DMD_ID IN ({', '.join(['?'] * len(medicine_code_list))}) "
    if clinical_code_list:
        where_clause += (
            f"AND CTV3Code IN ({', '.join(['?'] * len(clinical_code_list))}) "
        )
        coded_event_join = "INNER JOIN CodedEvent ON CodedEvent.Patient_ID = MedicationIssue.Patient_ID"
    if strict_codes:
        partition = "Patient_ID, DMD_ID"
        dmd_col = "DMD_ID, "
    else:
        partition = "Patient_ID"
        dmd_col = ""
    sql = f"""
    WITH
    issues
      AS (
       SELECT DISTINCT MedicationIssue.Patient_ID,
        {dmd_col}
        StartDate,
        DATEDIFF(MONTH, '1900-01-01', StartDate) AS months
       FROM
         MedicationIssue
       INNER JOIN
         MedicationDictionary ON MedicationIssue.MultilexDrug_ID = MedicationDictionary.MultilexDrug_ID
       {coded_event_join}
       WHERE
         DMD_ID != 'MULTIPLE_DMD_MAPPING'
         {where_clause}
       ),

    -- adds a column indicating how many months each row is from the previous row
    with_delta
      AS (
       SELECT *,
         LEAD(months) OVER (PARTITION BY {partition} ORDER BY months) - months AS delta
       FROM issues
       ),

    -- Whenever the delta is greater than a quarter, add a flag
    with_quarter_flag
      AS (
       SELECT *,
        CASE
         WHEN delta <= 3 -- this is three months *inclusive*
          THEN 0
         ELSE 1
         END AS outside_quarter
       FROM with_delta
       ),

    -- Use the flag to create a unique group id (`continuous_group`) for each non-adjacent group
    with_continuous_group
      AS (
       SELECT *,
        SUM(outside_quarter) OVER (
          PARTITION BY {partition} ORDER BY months ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS continuous_group
       FROM with_quarter_flag
       )

    -- Make the final table of patients, medicines, and dates
    SELECT
     {dmd_col}
     SUM(delta) AS continous_months,
     MIN(StartDate) AS start_date,
     Patient_ID
    FROM with_continuous_group
    WHERE
      delta <= 3 -- exclude groups with one row which is greater than one quarter
    GROUP BY
      {dmd_col}
      continuous_group,
      Patient_ID
    HAVING
      SUM(delta) >= {3 * (quarters - 1)} -- only include groups covering at least three quarters. It's `quarters - 1` because we count the *gaps*, not the *events*
    """
    return sql_to_df(sql, params=(medicine_code_list + clinical_code_list))


def update_many():
    """Just an example for the record
    """
    with closing_connection() as cnxn:
        cnxn.autocommit = False
        cursor = cnxn.cursor()
        params = [("1920-01-01", "1"), ("1998-01-01", "2")]
        cursor.executemany(
            "update Patient set DateOfDeath = ? where Patient_ID = ?", params
        )
        cnxn.commit()
