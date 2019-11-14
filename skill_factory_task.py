import requests
import json
import pymysql
import pymysql.cursors
import subprocess

subprocess.call([r'depends.bat'])


url = 'http://analytics.skillfactory.ru:5000/api/v1.0.1/get_structure_course/'    
print("Репликация в таблицу началась")  
res = requests.post(url)

response = json.loads(res.text)
response_dict = response["blocks"]

dict1 = {}

for key in response_dict.keys():
    if any(map(str.isdigit, response_dict[key]["display_name"])):
        dict1[response_dict[key]["block_id"]] = response_dict[key]["display_name"]
        if "children" in response_dict[key].keys():
            sub_list = response_dict[key]["children"]
            for sub_module in sub_list:
                if any(map(str.isdigit, response_dict[sub_module]["display_name"])):
                    dict1[response_dict[sub_module]["block_id"]] = response_dict[sub_module]["display_name"]
                else:
                    pass
    else:
	    pass


sorted_list = sorted(dict1.items(), key = lambda kv:(kv[1]))
dict2 = dict(sorted_list)


connection = pymysql.connect(host='localhost', #Указать свой адрес сервера
                             user='root', #Указать свой адрес логин
                             password='68popugaev)))') #Указать свой адрес пароль

connection.cursor().execute('drop database IF EXISTS testdb')
connection.cursor().execute('create database testdb')
connection.cursor().execute('use testdb')

try:
    with connection.cursor() as cursor:
        sql = "DROP TABLE IF EXISTS modules;"
        cursor.execute(sql)
        connection.commit()		
        sql = "CREATE TABLE `modules` (`id` int(11) NOT NULL AUTO_INCREMENT, `module_name` varchar(255) COLLATE utf8_bin NOT NULL, `module_id` varchar(255) COLLATE utf8_bin NOT NULL, `upload_date` DATETIME, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 AUTO_INCREMENT=1;"
        cursor.execute(sql)
        connection.commit()
	
        for key in dict2.keys():
            sql = "INSERT INTO `modules` (`module_id`, `module_name`, `upload_date`) VALUES (%s, %s, NOW())"
            cursor.execute(sql, (key, dict2[key]))
    connection.commit()
finally:
    connection.close()
    print("Репликация в таблицу завершена")
