#!flask/bin/python
from flask import Flask
from flask_restful import Resource, Api
from flask_restful import reqparse
import json
import oci
import sys
import os
import os.path
import io
import ast
from oci.object_storage.models import CreateBucketDetails
import cx_Oracle


app = Flask(__name__)
api = Api(app)

class GetAllItems(Resource):
    def get(self):
        try:
            connection = cx_Oracle.connect('username', 'password#', '<tns>') 
            cursor=connection.cursor();
            sql = "select * from airdata_passenger_movement_2001"
            rs = cursor.execute(sql)
            row_headers=[x[0] for x in rs.description]
            print(rs)
            json_data=[]
            for result in rs:
                json_data.append(dict(zip(row_headers,result)))

            results = [list(i) for i in rs]
            return {'StatusCode':'200','Items':json.dumps(json_data)}
        except Exception as e:
            return {'error': str(e)}



api.add_resource(GetAllItems, '/GetAllItems')


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, threaded=True, debug=True)