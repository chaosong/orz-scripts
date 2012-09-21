#!/home/q/python2/bin/python
# encoding: utf8

import sys
import pymysql


con = pymysql.connect(
    host='127.0.0.1',
    port=3306,
    user='username',
    passwd='password',
    db='hive_meta_db'
)


def get_table_info(tbl_name):
    sql = "select * from TBLS where TBL_NAME = %s"
    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql, (tbl_name, ))
    r = cur.fetchone()
    return r


def get_column_info(sd_id):
    sql = "select COLUMN_NAME, TYPE_NAME, COMMENT from COLUMNS where SD_ID = %s order by INTEGER_IDX"
    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql, (sd_id, ))
    rows = [(r['COLUMN_NAME'], r['TYPE_NAME'], r['COMMENT']) for r in cur.fetchall()]
    return rows


def get_partition_info(tbl_id):
    sql = "select * from PARTITION_KEYS where TBL_ID = %s order by INTEGER_IDX"
    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql, (tbl_id, ))
    rows = [(r['PKEY_NAME'], r['PKEY_TYPE'], r['PKEY_COMMENT']) for r in cur.fetchall()]
    return rows


def get_serde_info(sd_id):
    sql = "select * from SDS where SD_ID = %s"
    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql, (sd_id, ))
    r = cur.fetchone()
    return r


def get_delim_info(serde_id):
    sql = "select * from SERDE_PARAMS where SERDE_ID = %s"
    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql, (serde_id, ))
    result = {}
    for r in cur.fetchall():
        k = r['PARAM_KEY']
        v = r['PARAM_VALUE']
        result[k] = v
    return result


def get_load_parts(tbl_id):
    sql = "select PART_NAME from PARTITIONS where TBL_ID = %s order by PART_ID desc limit 3"
    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute(sql, (tbl_id, ))
    parts = [r['PART_NAME'] for r in cur.fetchall()]
    return parts


def get_part_str(part):
    parts = part.split('/')
    result = []
    for p in parts:
        k, v = p.split('=')
        result.append("%s='%s'" % (k, v))
    return ', '.join(result)


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Enter the table name !')
        exit()
    tbl_name = sys.argv[1]

    table_info = get_table_info(tbl_name)
    if table_info is None:
        print('No table found')
        exit()

    # 获取列信息
    column_info = get_column_info(table_info['SD_ID'])
    # 获取分区信息
    partition_info = get_partition_info(table_info['TBL_ID'])
    # 获取序列化信息
    serde_info = get_serde_info(table_info['SD_ID'])
    # 获取分隔符信息
    delim_info = get_delim_info(serde_info['SERDE_ID'])

    # drop table
    print("drop table %s;" % tbl_name)
    print('')

    # create table
    tbl_type = 'external' if table_info['TBL_TYPE'] == 'EXTERNAL_TABLE' else ''
    print("create %s table %s" % (tbl_type, tbl_name))
    # 打印列信息
    print('(')
    column_count = len(column_info)
    for i in range(column_count):
        name, type, comment = column_info[i]
        print '  ', name, type, 'comment ', "'%s'" % (comment or ''),
        if i < column_count - 1:
            print(',')
        else:
            print('')
    print(')')

    # 打印分区信息
    if partition_info is not None:
        print('partitioned by ')
        print('(')
        partition_count = len(partition_info)
        for i in range(partition_count):
            name, type, comment = partition_info[i]
            print '  ', name, type,
            if i < partition_count - 1:
                print(',')
            else:
                print('')
        print(')')

    # 打印序列化信息
    print('row format delimited')
    print("fields terminated by '\%03d'" % ord(delim_info['field.delim']))
    print("lines terminated by '\%03d'" % ord(delim_info['line.delim']))

    if 'org.apache.hadoop.mapred.SequenceFileInputFormat' == serde_info['INPUT_FORMAT']:
        stored_type = 'sequencefile'
    else:
        stored_type = 'textfile'
    print("stored as %s" % stored_type)
    print("location '%s';" % serde_info['LOCATION'])
    print('')

    # add partition sql
    add_partition_sql = "alter table %s add partition (%s) \n  location '%s';"
    parts = get_load_parts(table_info['TBL_ID'])
    for part in parts:
        part_str = get_part_str(part)
        print(add_partition_sql % (tbl_name, part_str, serde_info['LOCATION'] + '/' + part))

    print('')
    print('exit;')
