import unittest
from modules.CongestedEdge import CongestedEdge
from modules.Algorithm import Algorithm
from modules.MB_root import MB_root

# values for the simulation
from_id = 837
to_id = 506
p = 0.85
count_limit = 5
strTime = "09:30:00"


def printComparisons(root):  # prints the results compared
    mb = root.mb
    flow_model = mb.flow_model
    flow = flow_model(CongestionModelTestCase.algorithm.timeToInt(CongestionModelTestCase.root))
    density_model = mb.density_model
    density = density_model(CongestionModelTestCase.algorithm.timeToInt((CongestionModelTestCase.root)))
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
        printComparisons(i)


class CongestionModelTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):  # initializes the test case

        # initialize edge list
        cls.congested_edge = CongestedEdge(from_id, to_id, p)
        # initialize and run algorithm
        cls.algorithm = Algorithm(CongestionModelTestCase.congested_edge, strTime, count_limit)
        cls.algorithm.runAlgorithm()
        cls.root = cls.algorithm.master_root

    def test_printRoot(self):  # prints the initialized data
        print("****")
        print(CongestionModelTestCase.root)

    def test_compareValues(self):  # prints results
        print("****")
        printComparisons(CongestionModelTestCase.root)

    def test_densities(self):  # tests if the estimated densities  make sense
        print("****")
        root = CongestionModelTestCase.root

        density_model = root.mb.density_model
        k = density_model(CongestionModelTestCase.algorithm.timeToInt(root))
        k_new = root.k_new

        if (root.master > 0):
            self.assertLess(k_new, k,
                            msg="forward node (" + str(
                                root.node_id) + "): estimated density less than expected density")
        else:
            self.assertGreater(k_new, k, msg="backward node (" + str(
                root.node_id) + "): estimated density greater that expected density")

        for i in root.adjacent_roots:
            density_model = i.mb.density_model
            k = density_model(CongestionModelTestCase.algorithm.timeToInt(i))
            k_new = i.k_new
            if (i.master > 0):
                self.assertLess(k_new, k, msg="forward node (" + str(
                    i.node_id) + "): estimated density less than expected density")
            else:
                self.assertGreater(k_new, k, msg="backward node (" + str(
                    i.node_id) + "): estimated density greater that expected density")
            print("--------------------")

    def test_speeds(self):
        maxSpeed = 130
        minSpeed = 10
        print("****")
        root = CongestionModelTestCase.root

        self.assertLess(root.v_new, maxSpeed,
                        msg="forward node (" + str(root.node_id) + "): estimated density less than expected density")
        self.assertGreater(root.v_new, minSpeed, msg="backward node (" + str(
            root.node_id) + "): estimated density greater that expected density")

        for i in root.adjacent_roots:
            self.assertLess(i.v_new, maxSpeed,
                            msg="forward node (" + str(i.node_id) + "): estimated density less than expected density")
            self.assertGreater(i.v_new, minSpeed, msg="backward node (" + str(
                i.node_id) + "): estimated density greater that expected density")
            print("--------------------")

    def test_modelAccuracy(self):
        print("****")
        root = CongestionModelTestCase.root
        for i in root.adjacent_roots:
            mb_i = i.mb
            self.assertGreater(mb_i.flow_model_r2, 0.80, msg=str(i.node_id) + ": flow model r2 greater that 0.80")
            self.assertGreater(mb_i.density_model_r2, 0.80, msg=str(i.node_id) + ": density model r2 greater that 0.80")
            print("--------------------")


if __name__ == '__main__':
    unittest.main()
