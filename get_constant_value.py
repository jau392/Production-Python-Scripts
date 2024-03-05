#!/bin/env python3

import os
import sys
import re
import cs_db
import cs_environment
import cx_Oracle
from cs_logging import logmsg

def get_constant_value(constant_cd, logfile=None):
    db = cs_db.DataBase()
    
    # Establish generic Oracle acct to use
    if os.getenv("CS_PROD") == 'D':
        db_user = 'OracleInteractive_Prod'
    else:
        db_user = 'OracleInteractive'
    
    # Connect to Oracle
    conn, cursor = db.oracle_connect(db_user, AlwaysUseGenericID=1)
    if conn == None:
        logmsg("Unable to connect to database")
        sys.exit(1)
    
    # Run SQL to query for constant_value
    sql = "SELECT char_constant_tx FROM um_constants WHERE UPPER(constant_cd) = UPPER('{}')".format(constant_cd)
    try:
        cursor.execute(sql)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        logmsg("ERROR: Oracle-Error-Message: {0}\n".format(error.message), logfile)
    
    data = cursor.fetchall()
    if data:
        if len(data[0]) > 1:
            logmsg("ERROR: More than 1 result, unable to determine correct constant value", logfile)
            return None

        constant_value = data[0][0]
    
    # Change from Windows format to UNIX format
    constant_value = convert_windows_path_to_unix(constant_value)
    return constant_value

def convert_windows_path_to_unix(path):
    unix_path = re.sub(r'\\', '/', path)
    unix_path = unix_path.replace('//sicrops.schwab.com/', '/NAS/')
    unix_path = unix_path.replace('//nas30u0pdv.dev.schwab.com/', '/NAS/')
    unix_path = unix_path.replace('//nas30u0a2b.cdc.schwab.com/', '/NAS/')
    unix_path = unix_path.replace('//nas30u0cdc.cdc.schwab.com/', '/NAS/')
    return unix_path
    
    
def substitute_destination(dest_variable_string, logfile=None):
    if '%dest_' not in dest_variable_string.lower():
        return dest_variable_string
    if dest_variable_string.lower().index('%dest_') < 0:
        return dest_variable_string
    
    while dest_variable_string.lower().index('%dest_') >= 0:
        # Get the start position of the first occurance of %dest_
        start = dest_variable_string.lower().index('%dest_')
        # Get the end position of the first occurance of %dest_
        end = dest_variable_string.lower().index('%', start+1)
        # Compute the difference of the end position and the start position
        diff = end - start
        # Save the full string to be replaced, including the '%' signs
        dyn_dest_string = dest_variable_string[start:diff+1]
        # Get the destination value
        str_replace = get_constant_value(dyn_dest_string)
        
        if not str_replace:
            logmsg("ERROR: Unable to get constant value for the provided string: {}".format(dest_variable_string), logfile)
            return
        
        # Replace the dynamic destination string with the proper value from the database, and return
        new_string = re.sub(dyn_dest_string, str_replace, dest_variable_string)
        dest_variable_string = new_string
        
        # Break loop if dest_variable_string no longer contains '%dest'
        if '%dest_' not in dest_variable_string.lower():
            break
        
    return dest_variable_string