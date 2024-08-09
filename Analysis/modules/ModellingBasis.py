import pandas as pd
import numpy as np
from sklearn.metrics import r2_score
from modules.Cleaner import Cleaner
from scipy.misc import derivative

# defining constant variables
fTag = "flowvalue"
sTag = "speedvalue"
dTag = "measurementdatetime"
kTag = "density"
fitDegree = 16


def density(q, v):
    return q / v


def densityList(qList, vList):
    d = []
    for i in range(len(qList)):
        if (qList[i] != None and vList[i] != None):
            d.append(density(qList[i], vList[i]))
    return d


class ModellingBasis:

    def __init__(self, dataset):

        # concatenating datasets
        flowSeries = pd.concat([dataset[dTag], dataset[fTag]], axis=1, sort=False)
        speedSeries = pd.concat([dataset[dTag], dataset[sTag]], axis=1, sort=False)

        # cleaning data
        cleaner = Cleaner()
        flowSeries = cleaner.cleanFlow(flowSeries)
        speedSeries = cleaner.cleanSpeed(speedSeries)

        # creating dictionaries to match dates with integers
        flowDict = {}
        for index, row in flowSeries.iterrows():
            time = row[dTag].time()
            if (flowDict.get(time) == None):
                flowDict[time] = [row[fTag]]
            else:
                flowDict[time].append(row[fTag])

        speedDict = {}
        for index, row in speedSeries.iterrows():
            time = row[dTag].time()
            if (speedDict.get(time) == None):
                speedDict[time] = [row[sTag]]
            else:
                speedDict[time].append(row[sTag])

        # sorting the dictionaries
        newFlowDict = {}
        for key in sorted(flowDict):
            newFlowDict[key] = flowDict.get(key)

        flowDict = newFlowDict

        newSpeedDict = {}
        for key in sorted(speedDict):
            newSpeedDict[key] = speedDict.get(key)

        speedDict = newSpeedDict

        # converting dictionaries into dataframes
        self.flowDF = pd.DataFrame(columns=[dTag, fTag])
        for key in flowDict:
            for i in flowDict.get(key):
                flowAuxDF = pd.DataFrame([[key, i]], columns=[dTag, fTag])
                self.flowDF = self.flowDF.append(flowAuxDF, ignore_index=True)

        self.speedDF = pd.DataFrame(columns=[dTag, sTag])
        for key in speedDict:
            for i in speedDict.get(key):
                speedAuxDF = pd.DataFrame([[key, i]], columns=[dTag, sTag])
                self.speedDF = self.speedDF.append(speedAuxDF, ignore_index=True)

        # creating density list
        self.densityList = densityList(self.flowDF[fTag], self.speedDF[sTag])

        # generating flow model
        flow_y = np.array(self.flowDF.iloc[:, 1])
        flow_x = np.array(range(len(self.flowDF)))
        flow_coefs = np.polyfit(flow_x, flow_y, fitDegree)
        self.flow_model = np.poly1d(flow_coefs)
        self.flow_model_r2 = r2_score(flow_y, self.flow_model(flow_x))

        # generating density model
        try:
            density_y = np.array(self.densityList)
            density_x = np.array(range(len(self.densityList)))
            density_coefs = np.polyfit(density_x, density_y, fitDegree)
            self.density_model = np.poly1d(density_coefs)
            self.density_model_r2 = r2_score(density_y, self.density_model(density_x))
        except np.linalg.LinAlgError:
            print("ERROR: linear least squares, density model could not be generated")

        # finding optimal speed
        data = pd.concat([self.flowDF, self.speedDF], axis=1, sort=False)
        topFlows = data[data[fTag] > data.quantile(0.999)[0]]
        self.optSpeed = topFlows[sTag].mean()
        self.maxFlow = topFlows[fTag].mean()

        # calculating flow speed model
        selData = data[(data[sTag] < self.speedDF.quantile(0.99)[0]) & (data[sTag] > self.speedDF.quantile(0.01)[0]) & (
                    data[fTag] > self.flowDF.quantile(0.25)[0])]
        sel_flow = np.array(selData.iloc[:, 1])
        sel_speed = np.array(selData.iloc[:, 3])
        flow_speed_coefs = np.polyfit(sel_speed, sel_flow, fitDegree / 2)
        self.flow_speed_model = np.poly1d(flow_speed_coefs)
        self.flow_speed_model_r2 = r2_score(sel_flow, self.flow_speed_model(sel_speed))

        # calculating flow density model and critical density
        sel_density = densityList(self.flowDF[fTag], self.speedDF[sTag])
        fdDF = pd.DataFrame(columns=[kTag, fTag])
        for index, row in self.flowDF.iterrows():
            if (sel_density[index] < np.percentile(sel_density, 99.9)):
                auxDF = pd.DataFrame([[sel_density[index], row[fTag]]], columns=[kTag, fTag])
                fdDF = fdDF.append(auxDF, ignore_index=True)

        flow_density_coefs = np.polyfit(fdDF[kTag], fdDF[fTag], fitDegree / 2)
        self.flow_density_model = np.poly1d(flow_density_coefs)
        self.flow_density_model_r2 = r2_score(fdDF[fTag], self.flow_density_model(fdDF[kTag]))

        optXs = []
        for i in range(int(min(fdDF[kTag])), int(max(fdDF[kTag]))):
            der = derivative(self.flow_density_model, i, dx=1e-6)
            if (der < 0):
                der = der * -1
            if (der < 10):
                optXs.append(i)
        if (len(optXs) > 0):
            self.criticalDensity = min(optXs)

        # calculating speed density model
        sel_density = densityList(self.flowDF[fTag], self.speedDF[sTag])
        sdDF = pd.DataFrame(columns=[kTag, sTag])
        for index, row in self.speedDF.iterrows():
            if (np.percentile(sel_density, 99.9) > sel_density[index] > np.percentile(sel_density, 25)):
                auxDF = pd.DataFrame([[sel_density[index], row[sTag]]], columns=[kTag, sTag])
                sdDF = sdDF.append(auxDF, ignore_index=True)

        speed_density_coefs = np.polyfit(sdDF[kTag], sdDF[sTag], fitDegree / 2)
        self.speed_density_model = np.poly1d(speed_density_coefs)
        self.speed_density_model_r2 = r2_score(sdDF[sTag], self.speed_density_model(sdDF[kTag]))

    @property
    def criticalDensity(self):
        if (hasattr(self, "_criticalDensity")):
            return self._criticalDensity
        else:
            return self.maxFlow / self.optSpeed

    @criticalDensity.setter
    def criticalDensity(self, value):
        self._criticalDensity = value

    def __repr__(self):
        kc = "no critical density found"
        os = "no optimal speed found"
        mf = "no maximum flow found"
        if (hasattr(self, "criticalDensity")):
            kc = self.criticalDensity
        if (hasattr(self, "optSpeed")):
            os = self.optSpeed
        if (hasattr(self, "maxFlow")):
            mf = self.maxFlow
        return "<critical density: " + str(kc) + ", optimal speed: " + str(os) + ", maximum flow: " + str(mf) + ">"
