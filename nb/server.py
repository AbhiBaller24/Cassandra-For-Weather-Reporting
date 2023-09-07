import grpc
from concurrent import futures
import station_pb2_grpc
import station_pb2
from cassandra.cluster import Cluster
from cassandra import Unavailable, ConsistencyLevel
import pandas as pd
    
    
class Station(station_pb2_grpc.StationServicer):
    
    def __init__(self):
        #Cassandra Session
        try:
            self.cluster = Cluster(['project-5-catmongers5-db-1', 'project-5-catmongers5-db-2', 'project-5-catmongers5-db-3'])
            self.session = self.cluster.connect()
        
            #make sure we are in weather 
            self.session.execute("use weather")
            #query to insert data (tmin, tmax and data based on id)
            self.insert_statement = self.session.prepare("""
            INSERT INTO stations (id, date, record)
            VALUES
            (?, ?, {tmin: ?, tmax: ?})
            """)
            # W value (RF is 3)
            self.insert_statement.consistency_level = ConsistencyLevel.ONE
            # query to get max temp
            self.max_statement = self.session.prepare("SELECT MAX(record.tmax) AS max FROM stations WHERE id = ?")
            #setting R to be 3 to make sure R+W=RF and our W = 1 and RF = 3
            self.max_statement.consistency_level = ConsistencyLevel.THREE

        except Exception as e:
            print(e)
        
    def RecordTemps(self, request, context):
        try:
            #inserting data from whatever in request
            self.session.execute(self.insert_statement, (request.station, request.date, request.tmin, request.tmax))
            return station_pb2.RecordTempsReply(error='')
        except (ValueError, Unavailable) as e:
            return station_pb2.RecordTempsReply(error=str(e))
            
    def StationMax(self, request, context):
        try:
            #find max temp of id provided in request (convert query results to pd df to extract max value from it
            maxQuery = pd.DataFrame(self.session.execute(self.max_statement, (request.station,)))
            maxQuery = maxQuery.at[maxQuery.index[0], 'max']
            return station_pb2.StationMaxReply(tmax=maxQuery, error='')
        except (ValueError, Unavailable) as e:
            return station_pb2.StationMaxReply(error=str(e))
            
            
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[("grpc.so_reuseport", 0)])
    station_pb2_grpc.add_StationServicer_to_server(Station(), server)
    print("listening on port 5440")
    server.add_insecure_port('[::]:5440')
    server.start()
    server.wait_for_termination()
    
if __name__ == '__main__':
    server()
