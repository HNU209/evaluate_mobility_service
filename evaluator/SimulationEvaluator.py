from haversine import haversine
from datetime import datetime
import pandas as pd

class SimulationEvaluator:
    def __init__(self, trip, wait, move, unit='km', start_time=0, end_time=1440):
        self.trip = trip
        self.wait = wait
        self.move = move
        self.unit = unit
        self.start_time = start_time
        self.end_time = end_time
        
        self._init_result()
        self.dt = datetime.now()
        
    def _init_result(self):
        self.total_waiting_time, self.total_walking_time = [], []
        self.total_veh_moving_time, self.total_veh_moving_dist = [], []
        
        self.mean_waiting_time, self.mean_walking_time = [], []
        self.mean_veh_moving_time, self.mean_veh_moving_dist = [], []
    
    def eval(self, init=False):
        total_waiting_time, total_walking_time, total_veh_moving_time, total_veh_moving_dist = 0, 0, 0, 0
        
        ## initialize result variables
        if init: self._init_result()
        
        ## calculate waiting time : person
        for i in self.wait:
            s_t, e_t = i['timestamp']
            if s_t < self.start_time or e_t > self.end_time: continue
            total_waiting_time += e_t - s_t
        
        ## calculate moving time : person
        for i in self.move:
            s_t, e_t = i['timestamp']
            if s_t < self.start_time or e_t > self.end_time: continue
            total_walking_time += e_t - s_t
        
        for i in self.trip:
            ## calculate moving time : vehicle
            s_t, e_t = i['timestamp'][0], i['timestamp'][-1]
            if s_t < self.start_time or e_t > self.end_time: continue
            total_veh_moving_time += e_t - s_t
            
            ## calculate moving distance : vehicle
            trip_lst = i['trip']
            for idx in range(len(trip_lst) - 1):
                s_t_point = trip_lst[idx][::-1]
                e_t_point = trip_lst[idx + 1][::-1]
                dist = haversine(s_t_point, e_t_point, unit=self.unit)
                total_veh_moving_dist += dist
                
        mean_waiting_time = total_waiting_time / len(self.wait)
        mean_walking_time = total_walking_time / len(self.move)
        mean_veh_moving_time = total_veh_moving_time / len(self.trip)
        mean_veh_moving_dist = total_veh_moving_dist / len(self.trip)
        
        self.to_save_array(
            total_waiting_time, total_walking_time, total_veh_moving_time, total_veh_moving_dist,
            mean_waiting_time, mean_walking_time, mean_veh_moving_time, mean_veh_moving_dist
        )
        
    def to_save_array(self, twaitt, twalkt, tvmovet, tvmoved, mwaitt, mwalkt, mvmovet, mvmoved):
        self.total_waiting_time.append(twaitt)
        self.total_walking_time.append(twalkt)
        self.total_veh_moving_time.append(tvmovet)
        self.total_veh_moving_dist.append(tvmoved)
        
        self.mean_waiting_time.append(mwaitt)
        self.mean_walking_time.append(mwalkt)
        self.mean_veh_moving_time.append(mvmovet)
        self.mean_veh_moving_dist.append(mvmoved)
    
    def to_csv(self):
        result = {
            'total_waiting_time': self.total_waiting_time,
            'total_walking_time': self.total_walking_time,
            'total_veh_moving_time': self.total_veh_moving_time,
            'total_veh_moving_dist': self.total_veh_moving_dist,
            
            'mean_waiting_time': self.mean_waiting_time,
            'mean_walking_time': self.mean_walking_time,
            'mean_veh_moving_time': self.mean_veh_moving_time,
            'mean_veh_moving_dist': self.mean_veh_moving_dist,
        }
        result_df = pd.DataFrame(result)
        prefix = self.dt.now().strftime("%Y-%m-%d-%H-%M-%S")
        result_df.to_csv(f'{prefix}-result.csv', index=False)