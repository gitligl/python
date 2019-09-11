# coding=utf-8
from confluent_kafka import Consumer, KafkaError
import pymongo, json, sys, getopt, time, pymysql, re, os
from multiprocessing import Process

# 连接kafka
def Kafka_Collect(address='192.168.101.13:9092', instance='example'):
    c = Consumer({
        'bootstrap.servers': address,
        'group.id': '0',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([instance])
    #c.subscribe(['^test.*'])
    return c

# 连接mongo
def Mongo_Collect(host='192.168.101.13', port=27017, db='esg', table='popsage'):
    mongoclient = pymongo.MongoClient(host=host, port=port)
    mongodb = mongoclient[db]
    mongotable = mongodb[table]
    return mongotable

# 连接mysql
def Mysql_Collect(host='192.168.101.13', port=13306, user='root', password='123456'):
    conn = pymysql.connect(host=host,port=port,user=user,password=password)
    return conn

# 每天晚上23:30全量备份数据库,保留3个sql备份文件
def mysql_full_backup(database='ligl'):
    backupdirname = "/data/mysql/backup/"
    if not os.path.isdir(backupdirname):
        os.mkdir(backupdirname)
    backupdbname = os.path.join(backupdirname, database)
    if not os.path.isdir(backupdbname):
        os.mkdir(backupdbname)
    while True:
        today_date = int(time.strftime("%Y%m%d",time.localtime()))
        accurate_time = int(time.strftime("%H%M%S",time.localtime()))
        time.sleep(1)
        if accurate_time == 233000:
            filename = os.path.join(backupdbname, database + "-" + str(today_date) + ".sql")
            command = "docker exec -it mysql-13 mysqldump -uroot -p123456 --database " + database + " > " + filename
            os.system(command)
            sqlfiles = os.listdir(backupdbname)
            for f in sqlfiles:
                if not os.path.isfile(os.path.join(backupdbname,f)):
                    sqlfiles.remove(f)
            num = len(sqlfiles)
            max_num = 0
            if num > 1:
                for f in sqlfiles:
                    current = int(f.lstrip(database+"-").rstrip(".sql"))
                    if today_date - current > max_num:
                        max_num = today_date - current
                        need_del = database + "-" + str(current) + ".sql"
                if num > 3:
                    os.remove(os.path.join(backupdbname,need_del))
            print(str(today_date)+"-"+str(accurate_time)+"全量备份一次")
        
# 实时同步binlog至mongo
def Sync_to_Mongo():
    c = Kafka_Collect()
    mongo = Mongo_Collect()
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        s = str(msg.value().decode('utf-8'))
        j = json.loads(s)
        mongo.insert(j)
        print('Received message: {}'.format(s))

def data_print(binlog_data):
    if binlog_data['type'] == "ERASE":
        db = binlog_data['database']
        table = binlog_data['table']
        pattern = "`"+table+"`"
        repl = db+"."+table
        src_sql = binlog_data['sql']
        if "." in src_sql:
            sql = src_sql
        else:
            sql = re.sub(pattern,repl,src_sql,count=1,flags=re.I) + ";"
        print(sql)
    elif binlog_data['type'] == "QUERY":
        sql = binlog_data['sql']+";"
        print(sql)
    elif binlog_data['type'] == "CREATE":
        db = binlog_data['database']
        table = binlog_data['table']
        pattern = "`"+table+"`"
        repl = db+"."+table
        src_sql = binlog_data['sql']
        if "." in src_sql:
            sql = src_sql
        else:
            sql = re.sub(pattern,repl,src_sql,count=1,flags=re.I) + ";"
        print(sql)
    elif binlog_data['type'] == "ALTER":
        sql = binlog_data['sql']+";"
        print(sql)
    elif binlog_data['type'] == "INSERT":
        selectors = binlog_data['data']
        for s in selectors:
            # 将mysqlType类型为int的字段,对应的值也相应修改改int
            for k,v in binlog_data['mysqlType'].items():
                if 'int' in v:
                    s[k] = int(s[k])
            columns = tuple(s.keys())
            values = tuple(s.values())
            sql = "INSERT INTO " + binlog_data['database']+"."+binlog_data['table']+str(columns).replace("'","")+" VALUES"+str(values)+";"
            print(sql)
    elif binlog_data['type'] == "UPDATE":
        src_data, dst_data = data_to_sql(binlog_data)
        for s in src_data:
            index_num = src_data.index(s)
            sql = "UPDATE " + binlog_data['database']+"."+binlog_data['table'] + " SET " + dst_data[index_num] + " WHERE " + s + ";"
            print(sql)
    elif binlog_data['type'] == "DELETE":
        sql_data = data_to_sql(binlog_data)
        for r in sql_data:
            sql = "DELETE FROM " + binlog_data['database']+"."+binlog_data['table'] + " WHERE " + r + ";"
            print(sql)

def data_restore(binlog_data):
    conn = Mysql_Collect()
    cursor = conn.cursor()
    if binlog_data['type'] == "ERASE":
        db = binlog_data['database']
        table = binlog_data['table']
        pattern = "`"+table+"`"
        repl = db+"."+table
        src_sql = binlog_data['sql']
        if "." in src_sql:
            sql = src_sql
        else:
            sql = re.sub(pattern,repl,src_sql,count=1,flags=re.I) + ";"
        print(sql)
        cursor.execute(sql)
        conn.commit()
    elif binlog_data['type'] == "QUERY":
        sql = binlog_data['sql']+";"
        print(sql)
    elif binlog_data['type'] == "CREATE":
        db = binlog_data['database']
        table = binlog_data['table']
        pattern = "`"+table+"`"
        repl = db+"."+table
        src_sql = binlog_data['sql']
        if "." in src_sql:
            sql = src_sql
        else:
            sql = re.sub(pattern,repl,src_sql,count=1,flags=re.I) + ";"
        print(sql)
        cursor.execute(sql)
        conn.commit()
    elif binlog_data['type'] == "ALTER":
        sql = binlog_data['sql']+";"
        print(sql)
        cursor.execute(sql)
        conn.commit()
    elif binlog_data['type'] == "INSERT":
        selectors = binlog_data['data']
        for s in selectors:
            # 将mysqlType类型为int的字段,对应的值也相应修改改int
            for k,v in binlog_data['mysqlType'].items():
                if 'int' in v:
                    s[k] = int(s[k])
            columns = tuple(s.keys())
            values = tuple(s.values())
            sql = "INSERT INTO " + binlog_data['database']+"."+binlog_data['table']+str(columns).replace("'","")+" VALUES"+str(values)+";"
            print(sql)
            cursor.execute(sql)
            conn.commit()
    elif binlog_data['type'] == "UPDATE":
        src_data, dst_data = data_to_sql(binlog_data)
        for s in src_data:
            index_num = src_data.index(s)
            sql = "UPDATE " + binlog_data['database']+"."+binlog_data['table'] + " SET " + dst_data[index_num] + " WHERE " + s + ";"
            print(sql)
            cursor.execute(sql)
            conn.commit()
    elif binlog_data['type'] == "DELETE":
        sql_data = data_to_sql(binlog_data)
        for r in sql_data:
            sql = "DELETE FROM " + binlog_data['database']+"."+binlog_data['table'] + " WHERE " + r + ";"
            print(sql)
            cursor.execute(sql)
            conn.commit()
    cursor.close()
    conn.close()

def data_to_sql(binlog_data):
    data = binlog_data["data"]
    mysqlType = binlog_data["mysqlType"]
    if binlog_data["type"] == "DELETE":
        r = []
        for d in data:
            ss = ''
            for k, v in d.items():
                if 'int' in mysqlType[k]:
                    ss = ss + "," + k + " = " + v
                else:
                    ss = ss + "," + k + " = " + "'" + v + "'"
            ss = ss.lstrip(",").replace(",", " and ")
            r.append(ss)
        return r
    elif binlog_data["type"] == "UPDATE":
        old = binlog_data["old"]
        r = []
        t = []
        for d in data:
            index_num = data.index(d)
            old_data = old[index_num]
            for k, v in old_data.items():
                old[index_num][k] = d[k]
                d[k] = v
            src = ''
            for k, v in d.items():
                if 'int' in mysqlType[k]:
                    src = src + "," + k + " = " + v
                else:
                    src = src + "," + k + " = " + "'" + v + "'"
            src = src.lstrip(",").replace(",", " and ")
            r.append(src)
            dst = ''
            for k, v in old[index_num].items():
                if 'int' in mysqlType[k]:
                    dst = dst + "," + k + " = " + v
                else:
                    dst = dst + "," + k + " = " + "'" + v + "'"
            dst = dst.lstrip(",")
            t.append(dst)
        return r, t
        

def page_query(query_filter=None, page_size=2, page_no=1, execute=None):
    mongo = Mongo_Collect()
    while True:
        skip = page_size * (page_no - 1)
        # 分页读取mongo内容
        page_record = mongo.find(query_filter).limit(page_size).skip(skip)
        n = 0
        for r in page_record:
            n += 1
#            print(r)
            if execute == "print":
                data_print(r)
            elif execute == "restore":
                data_restore(r)
        # 当n不再小于page_size的值的时候，退出循环
        if n < page_size:
            break
        # 循环一次，需要更新一次偏移量
        page_no += 1

def usage():
    print('使用说明:')
    print('-h | --help                        打印帮助文档')
    print('-c                                 此时为kafka客户端消费数据存至mongo')
    print('-p                                 打印出sql语句')
    print('-r                                 恢复数据')
    print('--start-time="%Y-%m-%d %H:%M:%S"   日期筛选，指定开始日期')
    print('--stop-time="%Y-%m-%d %H:%M:%S"    日期筛选，指定结束日期')
    print('--database=                        使用指定库筛选')
    print('--table=                           使用指定表筛选')

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print('请指定运行参数，通过-h|--help获取使用说明')
    else:
        if sys.argv[1] == "-c":
            p1 = Process(target=mysql_full_backup)
            p2 = Process(target=Sync_to_Mongo)
            p1.start()
            p2.start()
        else:
            opts, args = getopt.getopt(sys.argv[1:], "hpr",["help","start-time=","stop-time=","database=","table="])
            query_filter = {}
            for op, value in opts:
                if op == "--start-time":
                    es = int(time.mktime(time.strptime(value,'%Y-%m-%d %H:%M:%S')))*1000
                    query_filter["es"] = {'$gte':es}
                elif op == "--stop-time":
                    ts = int(time.mktime(time.strptime(value,'%Y-%m-%d %H:%M:%S')))*1000
                    query_filter["ts"] = {'$lte':ts}
                elif op == "--database":
                    query_filter["database"] = value
                elif op == "--table":
                    query_filter["table"] = value
                elif op == "-p":
                    execute = "print"
                elif op == "-r":
                    execute = "restore"
                elif op == "-h" or op == "--help":
                    usage()
                    sys.exit()
            page_query(query_filter=query_filter,execute=execute)
