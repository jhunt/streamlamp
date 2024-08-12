import streamlit as st
import pandas as pd
import sqlite3

def authenticated():
  return 'sqlite3' in st.session_state and st.session_state['sqlite3'] is not None

def login_sidebar(default_path=None, default_user='sqlite3'):
  with st.sidebar:
    st.title('Connect to SQLite3')
    path = st.text_input('Path', value=default_path)
    user = st.text_input('User', value=default_user)
    login = st.button('Login', type='primary')

    if login:
      st.session_state['sqlite3'] = {
        'path': path,
        'user': user
      }

def session():
  return sqlite3.connect(st.session_state['sqlite3']['path'])

def execute(sql, params=()):
  '''Execute a single SQL statement for its side-effects (e.g. INSERT or DROP TABLE)'''
  db = session()
  c = db.cursor()
  c.execute(sql, params)
  db.commit()
  return c

def query(sql, params=None):
  '''Execute a SELECT statement and return a Pandas DataFrame of the resultset'''
  return pd.read_sql(sql, con=session(), params=params)

def query1(sql):
  '''Execute a SELECT statement, but return the first row, first column value.  Not intended for queries that (a) select multiple columns or (b) return more than one row.'''
  df = query(sql)
  if df.empty:
    return None
  return df[df.columns[0]][0]

def land(st, df, table, columns, debug=False):
  '''Land a Pandas DataFrame (df) into a HISTORIC_* base table, using the LANDING_* variant.  This clears out the LANDING_* table, writes the data frame to it, and then loads that into the HISTORIC_* table with appropriate loaded_by / loaded_at values.'''
  st.markdown(f'writing {df.shape[0]} results to `HISTORIC_{table.upper()}`...')

  execute(f'delete from landing_{table}')
  df.to_sql(f'landing_{table}', con=session(), if_exists='replace', index=False)
  sql = f'''
    insert into historic_{table}
      ({', '.join(columns)}, loaded_by, loaded_at)
    select {', '.join(columns)}, 'sqlite3', current_timestamp
      from landing_{table}
  '''
  if debug:
    st.markdown(f"```\n{sql}\n```")
  execute(sql)
  st.markdown(f'🎉 done!')

def replace(df, table, if_exists='replace'):
  execute(f'delete from {table}')
  df.to_sql(table, con=session(), if_exists=if_exists, index=False)
