
class CongestedEdge:

    def __init__(self, from_id, to_id, p):

        self.from_id = from_id
        self.to_id = to_id
        self.p = p

    def __repr__(self):
        return "<from id: " + str(self.from_id) + ", to id: " + str(self.to_id) + ", p: " + str(self.p) + ">"

