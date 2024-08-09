# constant variables
fTag = "flowvalue"
sTag = "speedvalue"


class Cleaner:

    def __init__(self):
        pass

    def cleanFlow(self, flowSeries):

        # dividing dataset into quantiles
        flowInterval1 = flowSeries.quantile(0.25)[0]
        flowInterval2 = flowSeries.quantile(0.50)[0]
        flowInterval3 = flowSeries.quantile(0.75)[0]
        flowInterval4 = flowSeries.quantile(1)[0]

        # defining mean variables for each of the quantile intervals
        flowMean1 = 0
        flowMean2 = 0
        flowMean3 = 0

        # as the sets may be unequally sized we define element count variables as well
        n1 = 0
        n2 = 0
        n3 = 0

        # getting the means of the three different intervals
        for index, row in flowSeries.iterrows():
            if (str(row[fTag]) != 'NaT'):
                if (row[fTag] > flowInterval1 and row[fTag] <= flowInterval2):
                    flowMean1 = flowMean1 + row[fTag]
                    n1 = n1 + 1
                elif (row[fTag] > flowInterval2 and row[fTag] <= flowInterval3):
                    flowMean2 = flowMean2 + row[fTag]
                    n2 = n2 + 1
                elif (row[fTag] > flowInterval3 and row[fTag] <= flowInterval4):
                    flowMean3 = flowMean3 + row[fTag]
                    n3 = n3 + 1

        if (n1 != 0):
            flowMean1 = flowMean1 / n1
        if (n2 != 0):
            flowMean2 = flowMean2 / n2
        if (n3 != 0):
            flowMean3 = flowMean3 / n3

        # cleaning flow data
        for index, row in flowSeries.iterrows():
            # first check if the zero value must be changed
            # the zero values which have the previous neighbor value above the first quantile will be changed
            if (row[fTag] == 0 or str(row[fTag]) == 'NaT'):
                if (index > 0):  # preventing from index out of bounds error
                    if (flowSeries.iloc[[index - 1][0]][
                        fTag] > flowInterval1):  # checking if the value is extremely low
                        if (flowSeries.iloc[[index - 1][0]][
                            fTag] > flowInterval3):  # checking the quantile which the previous neighbor belongs
                            flowSeries.set_value(index, fTag, flowMean3)
                        elif (flowSeries.iloc[[index - 1][0]][fTag] > flowInterval2):
                            flowSeries.set_value(index, fTag, flowMean2)
                        else:
                            flowSeries.set_value(index, fTag, flowMean1)

        return flowSeries

    def cleanSpeed(self, speedSeries):

        speedMean = speedSeries[speedSeries[sTag] > 0].mean()[sTag]  # mean

        # speed data cleaning
        for index, row in speedSeries.iterrows():
            if (row[sTag] <= 0):
                speedSeries.set_value(index, sTag, speedMean)

        return speedSeries
