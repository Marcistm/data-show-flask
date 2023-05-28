import json
from json import dumps

import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient, GEOSPHERE
from faker import Faker
import random
import datetime

app = Flask(__name__)
fake = Faker()
CORS(app, resources={r"/*": {"origins": "*"}})
client = MongoClient('mongodb://localhost:27017/')
db = client['location_tracking']
collection = db['location_data']
collection.create_index([('location', '2dsphere')])


@app.route('/generate_data')
def generate_data():
    for _ in range(1000):  # 生成1000条数据
        user_id = fake.uuid4()
        latitude = random.uniform(-90, 90)  # 随机生成纬度
        longitude = random.uniform(-180, 180)  # 随机生成经度
        timestamp = datetime.datetime.now()  # 当前时间戳
        data = {
            'user_id': user_id,
            'location': {
                'type': 'Point',
                'coordinates': [longitude, latitude]
            },
            'timestamp': timestamp
        }
        collection.insert_one(data)  # 插入数据到MongoDB
    return jsonify(code=200, msg='success'), 200


@app.route('/search', methods=['GET'])
def search():
    timestamp = request.args.get('time')
    latitude = request.args.get('latitude')
    longitude = request.args.get('longitude')
    nearby_users = query_users_nearby(timestamp, latitude, longitude, 1000000)
    return jsonify(code=200, msg='success', data=nearby_users), 200


def query_users_nearby(timestamp, latitude, longitude, distance):
    timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    start_time = timestamp - datetime.timedelta(minutes=1) + datetime.timedelta(hours=8)
    end_time = timestamp + datetime.timedelta(minutes=1) + datetime.timedelta(hours=8)
    query = {
        'timestamp': {
            '$gte': start_time,
            '$lt': end_time
        },
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': [float(longitude), float(latitude)]
                },
                '$maxDistance': distance
            }
        }
    }
    cursor = collection.find(query)
    documents = [json.loads(json.dumps(doc, default=str)) for doc in cursor]
    return documents


if __name__ == '__main__':
    app.run(debug=True)
