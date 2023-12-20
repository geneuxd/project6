#Server.py was built with the help from KJ choi
import grpc
from concurrent import futures
import station_pb2_grpc, station_pb2
from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import Cluster

cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
cass = cluster.connect()

class StationServicer(object):
    def RecordTemps(self, request, context):
        try:
            insert_statement = cass.prepare("""
            INSERT INTO weather.stations(id, date, record)
            VALUES(?,?,{tmax:?,tmin:?})
            """)
            insert_statement.consistency_level = ConsistencyLevel.ONE
            cass.execute(insert_statement, (request.station, request.date, request.tmin, request.tmax))
            return station_pb2.RecordTempsReply(error="")
        except Unavailable as e:
            return station_pb2.RecordTempsReply(error=f"need {e.required_replicas} replicas, but only have {e.alive_replicas}")
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        try:       
            max_statement = cass.prepare("""
                    SELECT max(record.tmax)
                    FROM weather.stations
                    WHERE id = ?
            """)
            max_statement.consistency_level = ConsistencyLevel.THREE
            tmax = cass.execute(max_statement, (request.station,))
            return station_pb2.StationMaxReply(tmax=tmax[0][0], error="")
        except Unavailable as e:
            return station_pb2.StationMaxReply(error=f"need {e.required_replicas} replicas, but only have {e.alive_replicas}")
        except Exception as e:
            return station_pb2.StationMaxReply(error=str(e))

server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
server.add_insecure_port("[::]:5440")
server.start()
server.wait_for_termination()