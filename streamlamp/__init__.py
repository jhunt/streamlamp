import json
import re

def json_ok(s):
  '''Returns True of `s` can be parsed as valid JSON, and False otherwise.'''
  try:
    json.loads(s)
    return True
  except:
    return False

def spacer(st, lines=3):
  '''Inserts any number of blank lines, for spacing text and user interface components out better.'''
  for x in range(lines):
    st.text('')

def qw(s):
  return re.split(r'\s+', s.strip())
