from datetime import datetime
import pandas as pd
import googlemaps
import requests

class ComparisonEvaluator:
    def __init__(self, raw, **kwargs):
        """
        based_columns: 
            request_time: request time
            pickup_time: pickup time
            dropoff_time: dropoff time
            request_lat: request latitude
            request_lon: request longitude
            pickup_lat: pickup latitude
            pickup_lon: pickup longitude
            dropoff_lat: dropoff latitude
            dropoff_lon: dropoff longitude
        """
        
        self.raw = raw
        self.kwargs = kwargs
        self._based_cols = ['request_time', 'pickup_time', 'dropoff_time', 'request_lat', 'request_lon', 'pickup_lat', 'pickup_lon', 'dropoff_lat', 'dropoff_lon']
        
        # naver pathfinding api : taxi
        self.naver_client_id = 'your-naver-client-id'
        self.naver_client_secret = 'your-naver-client-secret-key'
        self.naver_api = "https://naveropenapi.apigw.ntruss.com/map-direction/v1/driving?"
        self.naver_headers = {'X-NCP-APIGW-API-KEY-ID': self.naver_client_id, 'X-NCP-APIGW-API-KEY': self.naver_client_secret}
        
        # google pathfiding api : bus
        self.google_client_secret = 'your-google-client-secret-key'
        
        # tmap api
        self.tmap_api_key = 'your-tmap-api-key'
        self.tmap_url = 'https://apis.openapi.sk.com/tmap/routes/prediction?version=1&resCoordType=WGS84GEO&reqCoordType=WGS84GEO&sort=index&callback=function'
        self.tmap_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'appKey': self.tmap_api_key
        }
        
        # initialize variables to store
        self._init_result()
        self.dt = datetime.now()
        
        # rename to dataframe
        self.rename_df()
        
        # initialize api client
        self.init_api_client()
        
        # base evaluation
        self.eval_base()
        
    def _init_result(self):
        self.type_lst = []
        self.total_waiting_time, self.total_walking_time = [], []
        self.total_veh_moving_time, self.total_veh_moving_dist, self.total_veh_moving_price = [], [], []
        
        self.mean_waiting_time, self.mean_walking_time = [], []
        self.mean_veh_moving_time, self.mean_veh_moving_dist, self.mean_veh_moving_price = [], [], []
    
    def init_api_client(self):
        # naver - X & tmap - X
        # google
        self.google_client = googlemaps.Client(self.google_client_secret)
    
    def rename_df(self):
        columns = {self.kwargs.get(col, col):col for col in self._based_cols}
        self.raw = self.raw.rename(columns=columns)
        self.raw[['request_time', 'pickup_time', 'dropoff_time']] = self.raw[['request_time', 'pickup_time', 'dropoff_time']].apply(lambda x : pd.to_datetime(x).dt.tz_convert(None))
        
    def eval_base(self):
        self.raw['waiting_time'] = (self.raw['pickup_time'] - self.raw['request_time']).dt.total_seconds() / 60
        self.raw['moving_time'] = (self.raw['dropoff_time'] - self.raw['pickup_time']).dt.total_seconds() / 60
        
        self.to_save_array(
            'raw', self.raw.waiting_time.sum(), 0, self.raw.moving_time.sum(),
            self.raw.straight_distance_km.sum(), self.raw.fare.sum(),
            self.raw.waiting_time.mean(), 0, self.raw.moving_time.mean(),
            self.raw.straight_distance_km.mean(), self.raw.fare.mean()
        )
    
    def eval_taxi_naver(self, data, dist_unit='km', time_unit='m'):
        x1, y1, x2, y2 = data['pickup_lon'], data['pickup_lat'], data['dropoff_lon'], data['dropoff_lat']
        url = f'{self.naver_api}start={x1},{y1}&goal={x2},{y2}'
    
        data = requests.get(url, headers=self.naver_headers).json()
        if data.get('code', 1) != 0: return {'code': 0}
    
        summary = data['route']['traoptimal'][0]['summary']
        distance = summary['distance'] / 1000 if dist_unit == 'km' else summary['distance']
        duration = summary['duration'] / 60000 if time_unit == 'm' else summary['duration'] / 1000
        fare = summary['taxiFare']
        
        return {
            'distance': distance,
            'duration': duration,
            'price': fare
        }
    
    def eval_taxi_tmap(self, data, dist_unit='km', time_unit='m'):
        def zero_padding(value):
            value = str(value)
            if len(value) == 2: return value
            else: return '0' + value
        
        x1, y1, x2, y2 = data['pickup_lon'], data['pickup_lat'], data['dropoff_lon'], data['dropoff_lat']
        
        yy = str(data['pickup_time'].year)
        mm = zero_padding(data['pickup_time'].month)
        dd = zero_padding(data['pickup_time'].day)
        
        hour = zero_padding(data['pickup_time'].hour)
        min = zero_padding(data['pickup_time'].minute)
        sec = zero_padding(data['pickup_time'].second)
        
        payload = {"routesInfo": {
            "departure": {
                "name": "test1",
                "lon": f"{x1}",
                "lat": f"{y1}",
                "depSearchFlag": "05"
            },
            "destination": {
                "name": "test2",
                "lon": f"{x2}",
                "lat": f"{y2}",
                "poiId": "1000559885",
                "rpFlag": "16",
                "destSearchFlag": "03"
            },
            "predictionType": "departure",
            "predictionTime": f"{yy}-{mm}-{dd}T{hour}:{min}:{sec}+0900",
            "searchOption": "00",
            "tollgateCarType": "car",
            "trafficInfo": "N"
        }}
        
        res = requests.post(self.tmap_url, json=payload, headers=self.tmap_headers).json()
        
        prop = res['features'][0]['properties']
        distance = prop.get('totalDistance', 0) / 1000 if dist_unit == 'km' else prop.get('totalDistance', 0)
        duration = prop.get('totalTime', 0) / 60 if time_unit == 'm' else prop.get('totalTime', 0)
        fare = prop.get('taxiFare', 0)
        
        return {
            'distance': distance,
            'duration': duration,
            'price': fare
        }
    
    def eval_public_transport(self, data, mode='transit'):
        x1, y1, x2, y2 = data['pickup_lat'], data['pickup_lon'], data['dropoff_lat'], data['dropoff_lon']
        coords_start = f'{x1},{y1}'
        coords_end   = f'{x2},{y2}'
        
        data = self.google_client.directions(coords_start, coords_end, mode=mode)
        
        result = []
        for step in data[0]['legs'][0]['steps']:
            distance = step['distance']['value'] / 1000 # km 단위
            duration = step['duration']['value'] / 60 # 분 단위
            if step.get('transit_details'):
                travel_type = step['transit_details']['line']['vehicle']['type']
            else:
                travel_type = step['travel_mode']
            
            result.append({
                'distance': distance,
                'duration': duration,
                'price': 0,
                'travel_type': travel_type
            })
        
        return {
            'waiting_time': 0,
            'walking_time': sum(list(map(lambda x: x['duration'] if x['travel_type'] == 'WALKING' else 0, result))),
            'veh_moving_time': sum(list(map(lambda x: x['duration'] if x['travel_type'] != 'WALKING' else 0, result))),
            'veh_moving_dist': sum(list(map(lambda x: x['distance'] if x['travel_type'] != 'WALKING' else 0, result))),
            'veh_moving_price': 0,
        }
    
    def eval_taxi_total(self, type='naver'):
        if type == 'naver':
            result = self.raw.apply(self.eval_taxi_naver, axis=1).to_list()
        elif type == 'tmap':
            result = self.raw.apply(self.eval_taxi_tmap, axis=1).to_list()
        
        veh_moving_time = list(map(lambda x: x['duration'], result))
        veh_moving_dist = list(map(lambda x: x['distance'], result))
        veh_moving_price = list(map(lambda x: x['price'], result))
        
        self.to_save_array(
            f'taxi-{type}', self.raw.waiting_time.sum(), 0, sum(veh_moving_time),
            sum(veh_moving_dist), sum(veh_moving_price),
            self.raw.waiting_time.mean(), 0,
            sum(veh_moving_time) / len(self.raw),
            sum(veh_moving_dist) / len(self.raw),
            sum(veh_moving_price) / len(self.raw),
        )

    def eval_public_transport_total(self):
        result = self.raw.apply(self.eval_public_transport, axis=1).to_list()
        
        waiting_time = list(map(lambda x: x['waiting_time'], result))
        walking_time = list(map(lambda x: x['walking_time'], result))
        veh_moving_time = list(map(lambda x: x['veh_moving_time'], result))
        veh_moving_dist = list(map(lambda x: x['veh_moving_dist'], result))
        veh_moving_price = list(map(lambda x: x['veh_moving_price'], result))
        
        self.to_save_array(
            'public_transport', sum(waiting_time), sum(walking_time), sum(veh_moving_time), sum(veh_moving_dist), sum(veh_moving_price),
            sum(waiting_time) / len(self.raw), sum(walking_time) / len(self.raw),
            sum(veh_moving_time) / len(self.raw), sum(veh_moving_dist) / len(self.raw), sum(veh_moving_price) / len(self.raw)
        )
    
    def to_save_array(self, type_, twaitt, twalkt, tvmovingt, tvmovingd, tvmovingp,
                      mwaitt, mwalkt, mvmovingt, mvmovingd, mvmovingp):
        self.type_lst.append(type_)
        self.total_waiting_time.append(twaitt)
        self.total_walking_time.append(twalkt)
        self.total_veh_moving_time.append(tvmovingt)
        self.total_veh_moving_dist.append(tvmovingd)
        self.total_veh_moving_price.append(tvmovingp)
        
        self.mean_waiting_time.append(mwaitt)
        self.mean_walking_time.append(mwalkt)
        self.mean_veh_moving_time.append(mvmovingt)
        self.mean_veh_moving_dist.append(mvmovingd)
        self.mean_veh_moving_price.append(mvmovingp)
    
    def to_csv(self):
        result = {
            'type': self.type_lst, 
            'total_waiting_time': self.total_waiting_time,
            'total_walking_time': self.total_walking_time,
            'total_veh_moving_time': self.total_veh_moving_time,
            'total_veh_moving_dist': self.total_veh_moving_dist,
            'total_veh_moving_price': self.total_veh_moving_price,
            
            'mean_waiting_time': self.mean_waiting_time,
            'mean_walking_time': self.mean_walking_time,
            'mean_veh_moving_time': self.mean_veh_moving_time,
            'mean_veh_moving_dist': self.mean_veh_moving_dist,
            'mean_veh_moving_price': self.mean_veh_moving_price,
        }
        result_df = pd.DataFrame(result)
        prefix = self.dt.now().strftime("%Y-%m-%d-%H-%M-%S")
        result_df.to_csv(f'{prefix}-result.csv', index=False)