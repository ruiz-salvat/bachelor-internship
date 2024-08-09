from modules.ModellingBasis import ModellingBasis
import pandas as pd
from pyhive import hive
import threading

trainDatesList = [("2019-11-26 00:00:00.0", "2019-11-30 00:00:00.0"),
                  ("2019-12-02 00:00:00.0", "2019-12-06 00:00:00.0"),
                  ("2019-12-10 00:00:00.0", "2019-12-11 00:00:00.0"),
                  ("2019-12-19 00:00:00.0", "2019-12-21 00:00:00.0"),
                  ("2019-12-23 00:00:00.0", "2019-12-25 00:00:00.0"),
                  ("2019-12-02 00:00:00.0", "2019-12-06 00:00:00.0"),
                  ("2019-12-10 00:00:00.0", "2019-12-11 00:00:00.0"),
                  ("2019-12-19 00:00:00.0", "2019-12-21 00:00:00.0"),
                  ("2020-01-02 00:00:00.0", "2020-01-04 00:00:00.0"),
                  ("2020-01-14 00:00:00.0", "2020-01-18 00:00:00.0"),
                  ("2020-01-20 00:00:00.0", "2020-01-25 00:00:00.0"),
                  ("2020-01-27 00:00:00.0", "2020-02-01 00:00:00.0"),
                  ("2020-02-03 00:00:00.0", "2020-02-08 00:00:00.0")]

rTag = "reference"
idTag = "id"
fiTag = "fromid"
tiTag = "toid"

'''
the master variable can take -1, 0 or 1
-1: is input node to the previous one
0: is master node
1: is output node to the previous one

'''


class MB_root:
    static_count = 0  # measures the depth of the node in the graph
    limit_stack = []  # counts the iterated branches
    count_limit = 3  # depth limit
    started_threads = []
    kc_new = -1

    def __init__(self, node_id, master, origin):

        self.count = MB_root.static_count
        MB_root.static_count = MB_root.static_count + 1

        self.mb = None
        self.node_id = node_id
        self.master = master
        self.origin = origin  # previous initialized node

        if (master == 0):
            # database connection
            self.conn = hive.Connection(host="192.168.1.241", port=10000, username="hive")
            cursor = self.conn.cursor()
            cursor.execute("use RoadNetwork")
            sql = "SELECT fromid, toid FROM edge"
            MB_root.edges = pd.read_sql(sql, self.conn)

            # initialize limit stack
            MB_root.limit_stack.append(MB_root.count_limit)

        # checking neighbors
        inputs = MB_root.edges[MB_root.edges[tiTag] == self.node_id]
        outputs = MB_root.edges[MB_root.edges[fiTag] == self.node_id]

        self.input_ids = []
        self.output_ids = []

        for index, row in inputs.iterrows():
            self.input_ids.append(int(row[fiTag]))

        for index, row in outputs.iterrows():
            self.output_ids.append(int(row[tiTag]))

        # checking branches
        branches_div = inputs.shape[0] + outputs.shape[0] - 1
        if (branches_div > 1):
            MB_root.limit_stack.append(MB_root.limit_stack[len(MB_root.limit_stack) - 1] / branches_div)
            MB_root.static_count = 0

        # initializing adjacent nodes
        self.adjacent_roots = []
        if (self.count < MB_root.limit_stack[len(MB_root.limit_stack) - 1]):

            for i in self.output_ids:
                if (hasattr(self.origin, "node_id")):  # checks if it's the master 0
                    if (i != self.origin.node_id):
                        self.adjacent_roots.append(MB_root(i, 1, self))
                else:
                    self.adjacent_roots.append(MB_root(i, 1, self))
            for i in self.input_ids:
                if (hasattr(self.origin, "node_id")):
                    if (i != self.origin.node_id):
                        self.adjacent_roots.append(MB_root(i, -1, self))
                else:
                    self.adjacent_roots.append(MB_root(i, -1, self))

            if (branches_div > 1):
                MB_root.limit_stack.pop()

        else:
            MB_root.static_count = 0

        self.k_new = -1
        self.q_new = -1
        self.v_new = -1

        print("object initialized: " + str(self.node_id) + ", " + str(self.master))

    def setDataToNodes(self):  # node 0 sets data to all the nodes in the graph

        if (self.master == 0):
            nodes = self.__allNodesInSubgraph()
            trainData = self.__loadData(trainDatesList)
            print("data loaded")
            for i in nodes:
                auxDF = trainData[i.node_id].reset_index()
                x = threading.Thread(target=i.setMB, args=(auxDF,))
                x.start()
                MB_root.started_threads.append(x)
        else:
            print("ERROR: attempting to set data to nodes from a non master node")

    def __allNodesInSubgraph(self):  # checks the nodes in the graph

        nodes = []
        for i in self.adjacent_roots:
            nodes = nodes + i.__allNodesInSubgraph()
        nodes.append(self)
        return nodes

    def __loadData(self, datesList):  # retrieves data from database

        references = self.__getGraphReferences()

        if (references is not None):
            sql = "SELECT reference, measurementdatetime, flowvalue, speedvalue FROM MeasurementPoint WHERE"
            for i in datesList:
                startDate = i[0]
                endDate = i[1]
                for index, row in references.iterrows():
                    sql = sql + " reference = '" + row[
                        rTag] + "' AND measurementdatetime > '" + startDate + "' AND measurementdatetime < '" + endDate + "' OR"
            sql = sql[:-3]
            data = pd.read_sql(sql, self.conn)

            dataSetsDict = {}
            for index, row in references.iterrows():
                dataSetsDict[row[idTag]] = data[data[rTag] == row[rTag]]

            return dataSetsDict
        else:
            return None

    def __getGraphReferences(self):  # gets the references in the graph

        if (self.master == 0):

            id_list = self.__getGraphIds()

            sql = "SELECT reference, id FROM reference WHERE id = "
            for i in id_list:
                sql = sql + str(i) + " OR id = "

            sql = sql[:-9]
            references = pd.read_sql(sql, self.conn)

            return references

        else:
            return None

    def __getGraphIds(self):  # gets the ids in the graph

        id_list = []

        for i in self.adjacent_roots:
            id_list = id_list + i.__getGraphIds()

        id_list.append(self.node_id)

        return id_list

    def setMB(self, dataset):  # calculates the modelling basis of the node with the given dataset
        try:
            self.mb = ModellingBasis(dataset)  # test data to be set
        except:
            print("Corrupted data at: " + str(self.node_id))
            self.mb = None
        print("modeling basis set to " + str(self.node_id))

    def __repr__(self):
        return str(self.node_id) + " <" + str(self.master) + "> " + str(self.mb) + " :\n" + str(self.adjacent_roots)
