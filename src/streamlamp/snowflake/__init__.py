import streamlit as st

def session():
    return st.session_state['snowflake']

def execute(sql, params=None):
  '''Execute a single SQL statement for its side-effects (e.g. INSERT or DROP TABLE)'''
  c = session().cursor()
  c.execute(sql, params)
  return c

def query(sql, params=None):
  '''Execute a SELECT statement and return a Pandas DataFrame of the resultset'''
  c = execute(sql, params)
  return c.fetch_pandas_all()

def query1(sql):
  '''Execute a SELECT statement, but return the first row, first column value.  Not intended for queries that (a) select multiple columns or (b) return more than one row.'''
  df = query(sql)
  if df.empty:
    return None
  return df[df.columns[0]][0]

def land(st, df, table, columns):
  '''Land a Pandas DataFrame (df) into a HISTORIC_* base table, using the LANDING_* variant.  This clears out the LANDING_* table, writes the data frame to it, and then loads that into the HISTORIC_* table with appropriate loaded_by / loaded_at and valid_from values.'''
  st.markdown(f'writing {df.shape[0]} results to `HISTORIC_{table.upper()}`...')

  execute(f'truncate table landing_{table}')
  write_pandas(session(), df, f'landing_{table}'.upper())
  execute(f'''
    insert into {database}.{schema}.historic_{table}
      ({', '.join(columns)},
       valid_from, loaded_by, loaded_at)
    select {', '.join(columns)},
           current_date(), current_user(), current_timestamp()
      from {database}.{schema}.landing_{table}
  ''')
  st.markdown(f'🎉 done!')
