import json

class NumericSummary:
    
    def __init__(self, flowMean, flowStd, speedMean, speedStd, densityMean, densityStd):
        
        self.flowMean = flowMean
        self.flowStd = flowStd
        self.speedMean = speedMean
        self.speedStd = speedStd
        self.densityMean = densityMean
        self.densityStd = densityStd
        
    def to_json(self):
        
        return json.dumps(json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4))
        
        