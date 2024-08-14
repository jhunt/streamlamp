import tempfile
import streamlit as st
import snowflake
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def authenticated():
  return 'snowflake' in st.session_state and st.session_state['snowflake'] is not None

def login_sidebar(default_account=None, default_database=None, default_role=None, default_warehouse=None, default_schema=None, default_query_tag='streamlamp/snowflake', default_username=None, via_mfa_passcode=False):
  with st.sidebar:
    st.title('Connect to Snowflake')
    account   = st.text_input('Account', placeholder = 'i.e. abc1234567.us-central1.gcp', value = default_account)
    username  = st.text_input('Username')

    if via_mfa_passcode:
      c1, c2 = st.columns(2)
      password  = c1.text_input('Password', type = 'password')
      passcode  = c2.text_input('MFA Passscode', placeholder = '12345')

      database  = c1.text_input('Database', value = default_database, placeholder = '(optional)')
      role      = c2.text_input('Role', value = default_role, placeholder = '(optional)')

    else:
      password  = st.text_input('Password', type = 'password')
      c1, c2 = st.columns(2)

      database  = c1.text_input('Database', value = default_database, placeholder = '(optional)')
      role      = c2.text_input('Role', value = default_role, placeholder = '(optional)')

    advanced  = st.expander('Advanced Settings')
    schema    = advanced.text_input('Schema', value = default_schema, placeholder = '(optional)')
    warehouse = advanced.text_input('Warehouse', value = default_warehouse, placeholder = '(optional)')
    query_tag = advanced.text_input('Query Tag', value = default_query_tag, placeholder = '(optional)')
    login = st.button('Login', type='primary')

    if login:
      if via_mfa_passcode:
        st.session_state['snowflake'] = snowflake.connector.connect(
          user = username,
          password = password,
          passcode = passcode,
          account = account,
          warehouse = warehouse,
          role = role,
          database = database,
          schema = schema,
          session_parameters = {
            'QUERY_TAG': query_tag,
          }
        )
      else:
        st.session_state['snowflake'] = snowflake.connector.connect(
          user = username,
          password = password,
          account = account,
          warehouse = warehouse,
          role = role,
          database = database,
          schema = schema,
          session_parameters = {
            'QUERY_TAG': query_tag,
          }
        )

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
  '''Land a Pandas DataFrame (df) into a HISTORIC_* base table, using the LANDING_* variant.  This clears out the LANDING_* table, writes the data frame to it, and then loads that into the HISTORIC_* table with appropriate loaded_by / loaded_at values.'''
  st.markdown(f'writing {df.shape[0]} results to `HISTORIC_{table.upper()}`...')

  execute(f'truncate table landing_{table}')
  write_pandas(session(), df, f'landing_{table}'.upper())
  execute(f'''
    insert into historic_{table}
      ({', '.join(columns)}, loaded_by, loaded_at)
    select {', '.join(columns)}, current_user(), current_timestamp()
      from landing_{table}
  ''')
  st.markdown(f'🎉 done!')

def replace(df, table):
  execute(f'truncate table {table}')
  write_pandas(session(), df, table.upper())

def put_uploaded_file(uf, stage='~'):
  with tempfile.NamedTemporaryFile(mode = 'wb') as tf:
    tf.write(uf.read())
    tf.seek(0)
    execute(f'put file://{tf.name} @{stage}')

def put_uploaded_bytes(buf, stage='~'):
  with tempfile.NamedTemporaryFile(mode = 'wb') as tf:
    tf.write(buf)
    tf.seek(0)
    execute(f'put file://{tf.name} @{stage}')
