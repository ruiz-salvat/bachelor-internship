from modules.MB_root import MB_root
import time as t
from datetime import datetime
from time import mktime

dTag = "measurementdatetime"
waiting_time = 5


class Algorithm:

    def __init__(self, congested_edge, strTime, count_limit):

        self.congested_edge = congested_edge
        self.strTime = strTime
        MB_root.count_limit = count_limit
        self.master_root = None

    def __checkInit(self, root):
        seconds = 0
        k = False  # TRUE if all the threads are terminated
        while k is False:
            k = True
            active_threads = 0
            for i in root.started_threads:
                if (i.is_alive()):
                    k = False
                    active_threads += 1
            print("number of active threads: " + str(active_threads) + " (" + str(seconds) + "s)")
            seconds = seconds + waiting_time
            t.sleep(waiting_time)

        print("\n<initialization completed>\n")

    def runAlgorithm(self):  # run the whole process
        root = MB_root(self.congested_edge.from_id, 0, None)
        root.setDataToNodes()
        self.__checkInit(root)
        self.__first_backward(root, self.congested_edge.p)
        for j in root.adjacent_roots:
            if (j.node_id == self.congested_edge.to_id):
                self.__first_forward(j, self.congested_edge.p)
                break
        self.master_root = root

    def timeToInt(self, root):
        # taking the flow dataframe as the default one to transform a date into an integer
        time = t.strptime(self.strTime, "%H:%M:%S")
        time = datetime.fromtimestamp(mktime(time)).time()
        mb = root.mb
        flowDF = mb.flowDF
        timeInt = flowDF.shape[0] - 1
        for index, row in flowDF.iterrows():
            auxTime = row[dTag]
            if (auxTime >= time):
                timeInt = index
                return timeInt
        return timeInt

    def __first_backward(self, root, p):  # root is the node object and p the percentage of the available capacity
        kc = root.mb.criticalDensity  # critical density without the congestion
        kc_new = kc * p  # critical density with the congestion
        flow_model = root.mb.flow_model  # flow model describes the flow during the day
        q = flow_model(self.timeToInt(root))  # flow without congestion, timeToInt converts a string time to an integer
        q_new = q * p  # flow with congestion
        MB_root.kc_new = kc_new  # set new critical density of the first backward node
        root.q_new = q_new  # set new flow for the node
        for i in root.adjacent_roots:  # split again into partitions
            if (i.master < 0):
                self.__backward(i)
            if (i.master > 0):
                self.__forward(i)

        k_new = self.__calculateNewKBackward(root)  # calculates the estimated density
        root.k_new = k_new  # saving the estimated density to the node object
        root.v_new = q_new / k_new  # calculating estimated speed and saving it into the node object

    def __first_forward(self, root, p):
        flow_model = root.mb.flow_model  # flow model describes the flow during the day
        q = flow_model(self.timeToInt(root))  # flow without congestion
        q_new = q * p  # flow with congestion
        root.q_new = q_new  # set new flow for the node
        for i in root.adjacent_roots:  # make other forward partitions to estimate new densities
            self.__forward(i)

        k_new = self.__calculateNewKForward(root)  # calculates the estimated density
        root.k_new = k_new  # saving the estimated density to the node object
        root.v_new = q_new / k_new  # calculating estimated speed and saving it into the node object

    def __backward(self, root):

        root_origin = root.origin  # getting the origin node from this partition

        q_dict = {}  # dictionaries containing values later used in the formula
        kc_dict = {}

        for i in root_origin.adjacent_roots:  # getting values for the backward neighbours which aren't the current node
            if (
                    i.master < 0 and i != root):  # getMaster provide information about the partition type of the current node

                if (i.q_new > 0):  # checking if a new flow is already set
                    q_dict[i.node_id] = i.q_new  # setting flow value from adjacent node to the dictionary
                else:
                    q_dict[i.node_id] = i.mb.flow_model(
                        self.timeToInt(i))  # setting flow value from adjacent node to the dictionary

                kc_i_list = []
                for j in i.adjacent_roots:  # getting a list of the critical densities
                    if (j.master > 0 and j != i):
                        kc_i_list.append(j.mb.criticalDensity)
                kc_dict[i.node_id] = kc_i_list

        kc_i_list = []
        for i in root.adjacent_roots:  # getting a list of the critical densities of the forward nodes from the current nodes
            if (i != root_origin and i.master > 0):
                kc_i_list.append(i.mb.criticalDensity)
        kc_dict[root.node_id] = kc_i_list  # setting the list to the dictionary

        # the flow of the origin node must a new one
        # calculating new flow using the formula and its extracted inputs
        q_new = self.__backward_function(root.node_id, q_dict, kc_dict, root_origin.q_new, MB_root.kc_new)

        root.q_new = q_new  # setting the new flow to the node object

        for i in root.adjacent_roots:  # generating new partitions
            if (i.master < 0):
                self.__backward(i)
            if (i.master > 0):
                self.__forward(i)

        k_new = self.__calculateNewKBackward(root)  # calculates the estimated density
        root.k_new = k_new  # saving the estimated density to the node object
        root.v_new = q_new / k_new  # calculating estimated speed and saving it into the node object

    def __forward(self, root):

        mb = root.mb  # getting the origin node from this partition

        kc = mb.criticalDensity
        kc_dict = {}  # dictionaries containing values later used in the formula
        q_dict = {}

        for i in root.adjacent_roots:  # collecting values for the backward nodes

            if (i.master < 0):  # checking the backward adjacent nodes
                kc_i_list = []
                for j in i.adjacent_roots:
                    if (j.master > 0 and i != j):
                        kc_i_list.append(
                            j.criticalDensity)  # collecting the critical densities from the forward adjacent nodes
                kc_dict[i.node_id] = kc_i_list
                if (i.q_new > 0):  # setting the flows from the backward adjacent nodes to the dictionary
                    q_dict[i.node_id] = i.q_new
                else:  # if the new flow is not set to the node object
                    q_dict[i.node_id] = i.mb.flow_model(self.timeToInt(i))  # new flow

        kc_i_list = []
        for i in root.origin.adjacent_roots:  # collecting details for the origin node
            if (i.master > 0 and i.node_id != root.node_id):
                kc_i_list.append(i.mb.criticalDensity)
        kc_dict[root.origin.node_id] = kc_i_list
        if (root.origin.q_new > 0):
            q_dict[root.origin.node_id] = root.origin.q_new
        else:
            mb_origin = root.origin.mb
            flow_model_origin = mb_origin.q_new
            q_origin = flow_model_origin(self.timeToInt(root.origin))
            q_dict[root.origin.node_id] = q_origin

        # setting the new flow to the object nodes
        if (kc <= 0):
            kc = root.origin.mb.criticalDensity
            root.mb.criticalDensity = kc
        q_new = self.__forward_function(q_dict, kc_dict, kc)
        root.q_new = q_new

        for i in root.adjacent_roots:  # generate new partitions
            self.__forward(i)

        # setting the new speed and density to the object nodes
        k_new = self.__calculateNewKForward(root)
        root.k_new = k_new
        root.v_new = q_new / k_new

    def __forward_function(self, q_dict, kc_dict, kc):
        q = 0
        for key in q_dict:  # iterating through the dictionary (each adjacent node)
            kc_total = 0
            for i in kc_dict[key]:  # adding the critical densities (denominator) for each array in the dictionary
                kc_total = kc_total + i
            kc_total = kc_total + kc  # adding the critical density from origin as well
            try:
                q = q + (kc / kc_total) * q_dict[key]  # calculation new flow
            except:
                print("ERROR! two nodes with no critical density")
        return q

    def __backward_function(self, root_id, q_dict, kc_dict, q_origin, kc_origin):
        q = q_origin
        for key in q_dict:  # iterating through the dictionary (each adjacent node)
            kc_total = 0
            if (key != root_id):
                for i in kc_dict[key]:  # adding the critical densities (denominator) for each array in the dictionary
                    kc_total = kc_total + i
                kc_total = kc_total + kc_origin  # adding the critical density from origin as well
                q = q - (kc_origin / kc_total) * q_dict[key]  # qn - KCn / (KCn + KCk+1 + ... + KCkmax * qk)
        kc_total = 0
        for i in kc_dict[root_id]:  # adding the critical densities of the adjacent nodes from the current node
            kc_total = kc_total + i
        kc_total = kc_total + kc_origin  # adding the critical density from origin as well
        q = q * (kc_total / kc_origin)  # calculating new flow
        return q

    def __calculateNewKForward(self, root):  # calculate new density for forward nodes

        k_new = root.q_new / root.mb.optSpeed  # using optimal speed -> worst case

        return k_new

    def __calculateNewKBackward(self, root):  # calculate new density for backward nodes

        q = root.mb.flow_model(self.timeToInt(root))
        k = root.mb.density_model(self.timeToInt(root))

        k_new = k + self.__densityForCongestedState(q, root.q_new, MB_root.kc_new)

        return k_new

    def __densityForCongestedState(self, q, q_new, kc):  # density by flow difference
        k = -kc * (q_new / (60 * 60) - q / (60 * 60))  # transform to seconds
        if (k < 0):  # absolute value
            k = k * -1
        return k
