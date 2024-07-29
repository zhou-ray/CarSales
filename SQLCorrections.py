import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='',user='',password='',database='')
    if connection.is_connected():
        cursor = connection.cursor()

        create_table = """
        CREATE TABLE IF NOT EXISTS valid_states (
            abrv VARCHAR(2),
            name VARCHAR(255)
            )
            """
        cursor.execute(create_table)
        insert_query = "INSERT INTO valid_states (abrv,name) VALUES (%s, %s)"
        # including provinces
        state_list = [
            ('al','alabama'),('ak','alaska'),('az','arizona'),('ar','arkansas'),('ca','california'),('co','colorado'),
            ('ct','connecticut'),('de','delaware'),('fl','florida'),('ga','georgia'),('hi','hawaii'),('id','idaho'),
            ('il','illinois'),('in','indiana'),('ia','iowa'),('ks','kansas'),('ky','kentucky'),('la','louisiana'),
            ('me','maine'),('md','maryland'),('ma','massachusetts'),('mi','michigan'),('mn','minnesota'),
            ('ms','mississippi'),('mo','missouri'),('mt','montana'),('ne','nebraska'),('nv','nevada'),
            ('nh','new hampshire'),('nj','new jersey'),('nm','new mexico'),('ny','new york'),('nc','north carolina'),
            ('nd','north dakota'),('oh','ohio'),('ok','oklahoma'),('or','oregon'),('pa','pennsylvania'),
            ('ri','rhode island'),('sc','south carolina'),('sd','south dakota'),('tn','tennessee'),('tx','texas'),
            ('ut','utah'),('vt','vermont'),('va','virginia'),('wa','washington'),('wv','west virginia'),
            ('wi','wisconsin'),('wy','wyoming'),('ab','alberta'),('bc','british columbia'),('mb','manitoba'),
            ('nb','new brunswick'),('nf','newfoundland'),('nt','northwest territories'),('ns','nova scotia'),
            ('nu','nunavut'),('on','ontario'),('pe','prince edward island'),('qc','quebec'),('sk','saskatchewan'),
            ('yt','yukon')
        ]

        # sts = [(st,) for st in state_list]
        cursor.executemany(insert_query, state_list)

        delete_query = """
        DELETE FROM data 
        WHERE state NOT IN (SELECT abrv FROM valid_states)
        """
        cursor.execute(delete_query)
        print('Successful')
        connection.commit()

except Error as e:
    print(f"Error: {e}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()