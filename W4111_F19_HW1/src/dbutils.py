from src.BaseDataTable import BaseDataTable
import pymysql

import logging

logger = logging.getLogger()


def template_to_where_clause(template):
    """

    :param template: One of those weird templates
    :return: WHERE clause corresponding to the template.
    """

    if template is None or template == {}:
        result = (None, None)
    else:
        args = []
        terms = []

        for k, v in template.items():
            terms.append(" " + k + "=%s ")
            args.append(v)

        w_clause = "AND".join(terms)
        w_clause = " WHERE " + w_clause

        result = (w_clause, args)

    return result


def template_to_set_clause(template):
    """

    :param template: One of those weird templates
    :return: WHERE clause corresponding to the template.
    """

    if template is None or template == {}:
        result = (None, None)
    else:
        args = []
        terms = []

        for k, v in template.items():
            terms.append(" " + k + "=%s ")
            args.append(v)

        s_clause = "AND".join(terms)
        s_clause = " SET " + s_clause

        result = (s_clause, args)

    return result

def template_to_insert_clause(template):
    """

    :param template: One of those weird templates
    :return: WHERE clause corresponding to the template.
    """

    if template is None or template == {}:
        result = (None, None)
    else:
        args = []
        terms = []
        corres_terms = []
        for k, v in template.items():
            terms.append(" " + k )
            corres_terms.append(" "+ "%s")
            args.append(v)

        i_clause = "("+",".join(terms)+")"
        i_terms = "("+",".join(corres_terms)+")"

        result = (i_clause,i_terms, args)

    return result


def create_select(table_name, template, fields, order_by=None, limit=None, offset=None):
    """
    Produce a select statement: sql string and args.

    :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
    :param fields: Columns to select (an array of column name)
    :param template: One of Don Ferguson's weird JSON/python dictionary templates.
    :param order_by: Ignore for now.
    :param limit: Ignore for now.
    :param offset: Ignore for now.
    :return: A tuple of the form (sql string, args), where the sql string is a template.
    """

    if fields is None:
        field_list = " * "
    else:
        field_list = " " + ",".join(fields) + " "

    w_clause, args = template_to_where_clause(template)

    sql = "select " + field_list + " from " + table_name + " " + w_clause

    return (sql, args)


def create_delete(table_name, template, order_by=None, limit=None, offset=None):
    """
    Produce a select statement: sql string and args.

    :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
    :param fields: Columns to select (an array of column name)
    :param template: One of Don Ferguson's weird JSON/python dictionary templates.
    :param order_by: Ignore for now.
    :param limit: Ignore for now.
    :param offset: Ignore for now.
    :return: A tuple of the form (sql string, args), where the sql string is a template.
    """

    # if fields is None:
    #     field_list = " * "
    # else:
    #     field_list = " " + ",".join(fields) + " "

    w_clause, args = template_to_where_clause(template)

    sql = "delete " + " from " + table_name + " " + w_clause

    return (sql, args)


def create_update(table_name, template, new_template, order_by=None, limit=None, offset=None):
    """
    Produce a select statement: sql string and args.

    :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
    :param fields: Columns to select (an array of column name)
    :param template: One of Don Ferguson's weird JSON/python dictionary templates.
    :param order_by: Ignore for now.
    :param limit: Ignore for now.
    :param offset: Ignore for now.
    :return: A tuple of the form (sql string, args), where the sql string is a template.
    """
    w_clause, wargs = template_to_where_clause(template)
    s_clause, sargs = template_to_set_clause(new_template)
    sql = "update " + table_name + " " + s_clause + " " + w_clause

    return sql, sargs + wargs


def create_insert(table_name, template, order_by=None, limit=None, offset=None):
    """
    Produce a select statement: sql string and args.

    :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
    :param fields: Columns to select (an array of column name)
    :param template: One of Don Ferguson's weird JSON/python dictionary templates.
    :param order_by: Ignore for now.
    :param limit: Ignore for now.
    :param offset: Ignore for now.
    :return: A tuple of the form (sql string, args), where the sql string is a template.
    """

    i_clause, i_terms, iargs = template_to_insert_clause(template)
    sql = "INSERT INTO " + table_name+i_clause+ " VALUES" + i_terms
    return sql,iargs


def get_connection(connect_info):
    """
    :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
    :return: The connection. May raise an Exception/Error.
    """

    cnx = pymysql.connect(**connect_info)
    return cnx


def run_q(sql, args=None, fetch=True, cur=None, conn=None, commit=True):
    '''
    Helper function to run an SQL statement.

    :param sql: SQL template with placeholders for parameters.
    :param args: Values to pass with statement.
    :param fetch: Execute a fetch and return data.
    :param conn: The database connection to use. The function will use the default if None.
    :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
    :param commit: This is wizard stuff. Do not worry about it.

    :return: A tuple of the form (execute response, fetched data)
    '''
    try:

        if cur is None:
            cursor_created = True
            cur = conn.cursor()

        if args is not None:
            log_message = cur.mogrify(sql, args)
        else:
            log_message = sql

        logger.debug("Executing SQL = " + log_message)

        res = cur.execute(sql, args)

        if fetch:
            data = cur.fetchall()
        else:
            data = None

    except Exception as e:
        raise (e)
    conn.commit()
    return (res, data)
