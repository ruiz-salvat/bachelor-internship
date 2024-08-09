from modules.CongestedEdge import CongestedEdge
from modules.Algorithm import Algorithm
from modules.MB_root import MB_root


class Simulation:

    def __init__(self, from_id, to_id, p, count_limit, strTime):
        self.from_id = from_id
        self.to_id = to_id
        self.p = p
        self.count_limit = count_limit
        self.strTime = strTime

        congested_edge = CongestedEdge(self.from_id, self.to_id, self.p)
        self.algorithm = Algorithm(congested_edge, self.strTime, self.count_limit)

    def run_algorithm(self):

        self.algorithm.runAlgorithm()
        self.root = self.algorithm.master_root  # root object temporally set

    def print_result(self, root):
        mb = root.mb
        flow_model = mb.flow_model
        flow = flow_model(self.algorithm.timeToInt(root))
        density_model = mb.density_model
        density = density_model(self.algorithm.timeToInt((root)))
        print(str(root.node_id) + ":\n{maximum flow: " + str(mb.maxFlow) + ", expected flow: " + str(
            flow) + ", estimated flow: " + str(root.q_new) + "}")
        if (root.node_id == 0):
            kc = MB_root.kc_new
        else:
            kc = mb.criticalDensity
        print("{critical density: " + str(kc) + ", expected density: " + str(density) + ", estimated density: " + str(
            root.k_new) + "}")
        print("{optimal speed: " + str(mb.optSpeed) + ", estimated speed: " + str(root.v_new) + "}")
        print("--------------------")

        for i in root.adjacent_roots:
            self.print_result(i)
