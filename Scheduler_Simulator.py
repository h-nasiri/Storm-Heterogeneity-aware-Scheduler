import os
import copy
import math

# """"""""""Topology & Cluster Detailes""""""""""""""""


ComponentList = [0,1,2]

StreamMap = [[0,1],[1,2]]

DegreeInput = [[0, 0],[1, 1],[2,1]]

#DetailesForTopology = [[0, 1], [1, 2],[2,0],[3,0]]

DegreeOutput = [[0, 1], [1, 1],[2,0]]

'''#senario 2 Rc
TaskDetailes = [[0,0,295,10,160,10],[0,1,244,10,72,10],[0,2,277,10,60,40],[0,3,295,10,160,10],[0,4,244,10,72,10],[0,5,277,10,60,40],
                [0,6,295,10,160,10],[0,7,244,10,72,10],[0,8,277,10,60,40],
                [1,0,2872,1,1586,10],[1,1,2300,1,697,10],[1,2,1853,1,299,30],[1,3,2872,1,1586,10],[1,4,2300,1,697,10],[1,5,1853,1,299,30],
                [1,6,2872,1,1586,10],[1,7,2300,1,697,10],[1,8,1853,1,299,30]
               ]
'''


'''#senario 3 RC FOr Rc
TaskDetailes = [[0,0,295,10,160,10],[0,1,244,10,72,10],[0,2,277,10,60,40],[0,0,295,10,160,10],
                [0,1,244,10,72,10],[0,2,277,10,60,40],[0,1,244,10,72,10],[0,1,244,10,72,10],
                [0,1,244,10,72,10],
                [1,0,2872,1,1586,10],[1,1,2300,1,697,10],[1,2,1853,1,299,30],[1,3,2872,1,1586,10],[1,4,2300,1,697,10],[1,5,1853,1,299,30],
                [1,6,2300,1,697,10],[1,7,2300,1,697,10],[1,8,2300,1,697,10]
               ]
'''


'''#senario for all Machine AMD in RollingCount
TaskDetailes = [[0,0,277,10,60,40],[0,1,277,10,60,40],[0,2,277,10,60,40],[0,3,277,10,60,40],
                [0,4,277,10,60,40],[0,5,277,10,60,40],[0,6,277,10,60,40],[0,7,277,10,60,40],
                [0,8,277,10,60,40],
                [1,0,1853,1,299,30],[1,1,1853,1,299,30],[1,2,1853,1,299,30],[1,3,1853,1,299,30],[1,4,1853,1,299,30],[1,5,1853,1,299,30],
                [1,6,1853,1,299,30],[1,7,1853,1,299,30],[1,8,1853,1,299,30]
               ]
'''

#Senario for all Machine AMD in UniqueVisitor
'''
TaskDetailes = [[0,0,300,1,239,30],[0,1,300,1,239,30],[0,2,300,1,239,30],[0,3,300,1,239,30],
                [0,4,300,1,239,30],[0,5,300,1,239,30],[0,6,300,1,239,30],[0,7,300,1,239,30],
                [0,8,300,1,239,30],
                [1,0,381,0.5,91,40],[1,1,381,0.5,91,40],[1,2,381,0.5,91,40],[1,3,381,0.5,91,40],[1,4,381,0.5,91,40],[1,5,381,0.5,91,40],
                [1,6,381,0.5,91,40],[1,7,381,0.5,91,40],[1,8,381,0.5,91,40]
               ]

'''
#Senario 5 FOR RC
'''
TaskDetailes = [[0,0,277,10,60,40],[0,1,244,10,72,10],[0,2,295,10,160,10],[0,3,277,10,60,40],[0,4,244,10,72,10],[0,5,295,10,160,10],
				[0,6,277,10,60,40],[0,7,295,10,160,10],[0,8,277,10,60,40],
                [1,0,1853,1,299,30], [1,1,2300,1,697,10],[1,2,2872,1,1586,10],[1,3,1853,1,299,30],[1,4,2300,1,697,10],[1,5,2872,1,1586,10],[1,6,1853,1,299,30],[1,7,2872,1,1586,10],
				[1,8,1853,1,299,30]
               ]
'''

#Senario 5 FOR UV
'''
TaskDetailes = [[0, 0, 300, 1, 239, 30], [0, 1, 2806, 1, 680, 10], [0, 2, 2866, 1, 927, 10],

                [0, 3, 300, 1, 239, 30], [0, 4, 2806, 1, 680, 10], [0, 5, 2866, 1, 927, 10],

                [0, 6, 300, 1, 239, 30], [0, 7, 2866, 1, 927, 10], [0, 8, 300, 1, 239, 30],

                [1, 0, 381, 0.5, 91, 40], [1, 1, 2909, 0.5, 774, 10], [1, 2, 3013, 0.5, 790, 10],

                [1, 3, 381, 0.5, 91, 40], [1, 4, 2909, 0.5, 774, 10], [1, 5, 3013, 0.5, 790, 10],

                [1, 6, 381, 0.5, 91, 40], [1, 7, 3013, 0.5, 790, 10], [1, 8, 381, 0.5, 91, 40]
                ]

'''
'''
TaskDetailes = [[0,0,2866,1,927,10],[0,1,2866,1,927,10],[0,2,2866,1,927,10],
               [0,3,2866,1,927,10],[0,4,2866,1,927,10],
               [1,0,3013,0.5,790,10],[1,1,3013,0.5,790,10],[1,2,3013,0.5,790,10],
               [1,3,3013,0.5,790,10],[1,4,3013,0.5,790,10]
                ]
'''
'''
TaskDetailes = [[0,0,295,10,160,10],[0,1,295,10,160,10],[0,2,295,10,160,10],[0,3,295,10,160,10],[0,4,295,10,160,10],
				[1,0,2872,1,1586,10],[1,1,2872,1,1586,10],[1,2,2872,1,1586,10],[1,3,2872,1,1586,10],[1,4,2872,1,1586,10]
               ]
'''

TaskDetailes = [
                [0,0,128,1,0.0453,19.64],[1,0,24,1,0.0920,8],[2,0,19,1,0.1811,9.22]
                [0,1,16,1,0.099,3],[1,1,16,1,0.186,4],[2,1,16,1,0.345,7]
                ]

# [[ IdTask(0) , IdMachine(1) , InputRateDefault(2) , @(3) , Y(4), UtilizationDefault(5) ]]


TopologyOnCluster = []  # [[ IdTask(0) , IdMachine(1) , InputRate(2) , @(3) , Y(4) , Utilization(5) , #Instance(6) ]]

MachinesOfCluster = [[0, 100], [1, 100]]  # [[IdMachine(0) , UtilizatonAvalible]]

Numberr = 0
TestNumber = 0


# """""""""""""""""""end"""""""""""""""""""""""""""""""""


# """"""""""""""""""FirstAssign""""""""""""""""""""""""""

# """"""""""""""""""FirstAssign""""""""""""""""""""""""""
def FirstAssign():
    global ComponentList
    global StreamMap
    global DegreeInput
    global DegreeOutput
    global TaskDetailes
    global TopologyOnCluster
    global MachinesOfCluster

    global DetailesForTopology

    CountOfMachine = len(MachinesOfCluster)

    Matrix = sorted(TaskDetailes, key=lambda x: x[3], reverse=True)
    assignedTaskTemp = []

    # """""""""specify the location of all task in Cluster"""""""""
    for i in range(0, len(Matrix)):
        try:
            t = assignedTaskTemp.index(Matrix[i][0])
        except:
            assignedTaskTemp.append(Matrix[i][0])
            Max = -100
            MaxTemp = []
            for j in range(0, len(Matrix)):
                if (Matrix[i][0] == Matrix[j][0]):
                    if (Matrix[j][4] > Max):
                        Max = Matrix[j][4]
                        MaxTemp = Matrix[j]
            if (DegreeInput[MaxTemp[0]][1] == 0):
                TopologyOnCluster.append([MaxTemp[0], MaxTemp[1], MaxTemp[2], MaxTemp[3], MaxTemp[4], MaxTemp[5], 1])
                # [[ IdTask(0) , IdMachine(1) , InputRate(2) , @(3) , Y(4) , Utilization(5) , #Instance(6) ]]
                # [ IdTask(0) , IdMachine(1) , InputRateDefault(2) , @(3) , Y(4), UtilizationDefault(5) ]
                # CpuOfMachine[MaxTemp[1]] = [MaxTemp[1],CpuOfMachine[MaxTemp[1]][1] - MaxTemp[5]]
            else:
                TopologyOnCluster.append([MaxTemp[0], MaxTemp[1], MaxTemp[2], MaxTemp[3], MaxTemp[4], MaxTemp[5], 1])
                    # CpuOfMachine[MaxTemp[1]] = [MaxTemp[1],CpuOfMachine[MaxTemp[1]][1] - MaxTemp[5]]

    # """""""""specify input rate of all task in cluster"""""""""""
    for i in range(0, len(StreamMap)):
        task = StreamMap[i][0]
        if DegreeInput[task][1] == 0:
            TotalInputRate = 0
            NumberOfInstance = 0
            for j in range(0, len(TopologyOnCluster)):
                if task == TopologyOnCluster[j][0]:
                    TotalInputRate = TotalInputRate + TopologyOnCluster[j][2] * TopologyOnCluster[j][6]
                    NumberOfInstance += TopologyOnCluster[j][6]

            InputRateNew = int(TotalInputRate / NumberOfInstance)
            OutputRate = 0

            for k in range(0, len(TopologyOnCluster)):
                if task == TopologyOnCluster[k][0]:
                    InputRateOld = TopologyOnCluster[k][2]
                    UtilizationOld = TopologyOnCluster[k][5]
                    Y = TopologyOnCluster[k][4]
                    UtilizationNew = (((InputRateNew - InputRateOld) / Y) + UtilizationOld) * TopologyOnCluster[k][6]
                    TopologyOnCluster[k][5] = UtilizationNew
                    TopologyOnCluster[k][2] = InputRateNew

                    alfa = TopologyOnCluster[k][3]

                    OutputRate += InputRateNew * alfa * TopologyOnCluster[k][6]

            n = len(StreamMap[i]) - 1
            OutputRate = OutputRate / n
            NumberOfInstance = 0
            for k in range(1, len(StreamMap[i])):
                Successor = StreamMap[i][k]
                for l in range(0, len(TopologyOnCluster)):
                    if TopologyOnCluster[l][0] == Successor:
                        NumberOfInstance += TopologyOnCluster[l][6]
                InputRateNew = OutputRate / NumberOfInstance
                for m in range(0, len(TopologyOnCluster)):
                    if TopologyOnCluster[m][0] == Successor:
                        InputRateOld = TopologyOnCluster[m][2]
                        UtilizationOld = TopologyOnCluster[m][5]
                        Y = TopologyOnCluster[m][4]
                        CountOfInstance = TopologyOnCluster[m][6]
                        UtilizationNew = (((InputRateNew - InputRateOld) / float(Y)) + float(
                            UtilizationOld)) * CountOfInstance

                        TopologyOnCluster[m][2] = InputRateNew
                        TopologyOnCluster[m][5] = UtilizationNew


        elif DegreeOutput[task][1] != 0:
            OutputRate = 0
            for j in range(0, len(TopologyOnCluster)):
                if task == TopologyOnCluster[j][0]:
                    OutputRate = OutputRate + (
                    (TopologyOnCluster[j][2] * TopologyOnCluster[j][3]) * TopologyOnCluster[j][6])
            n = len(StreamMap[i]) - 1
            OutputRate = OutputRate / n
            NumberOfInstance = 0
            for k in range(1, len(StreamMap[i])):
                Successor = StreamMap[i][k]
                for l in range(0, len(TopologyOnCluster)):
                    if TopologyOnCluster[l][0] == Successor:
                        NumberOfInstance += TopologyOnCluster[l][6]
                InputRateNew = OutputRate / NumberOfInstance
                for m in range(0, len(TopologyOnCluster)):
                    if TopologyOnCluster[m][0] == Successor:
                        InputRateOld = TopologyOnCluster[m][2]
                        UtilizationOld = TopologyOnCluster[m][5]
                        Y = TopologyOnCluster[m][4]
                        CountOfInstance = TopologyOnCluster[m][6]
                        UtilizationNew = (((InputRateNew - InputRateOld) / float(Y)) + UtilizationOld) * CountOfInstance

                        TopologyOnCluster[m][2] = InputRateNew
                        TopologyOnCluster[m][5] = UtilizationNew
    # """"""""""""""""""""""""""""""""""""""end""""""""""""""""""""

    # """"""""""""""""""Utilization Of all  Machines in cluster""""
    for i in range(0, len(MachinesOfCluster)):
        Machine = MachinesOfCluster[i][0]
        UtilizationUsed = 0
        UtilizationAvailable = MachinesOfCluster[i][1]
        for j in range(0, len(TopologyOnCluster)):
            if Machine == TopologyOnCluster[j][1]:
                UtilizationUsed += TopologyOnCluster[j][5]
        if UtilizationUsed < UtilizationAvailable:
            MachinesOfCluster[i][1] = UtilizationAvailable - UtilizationUsed
        else:
            return -1
            # """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    # """""""""""""""""""print the Detalies of Topology and Machines"

    '''
    print "||Task ID||   " , "||Run On Machine||   " , "||InputRate||   "  , "||Utilization Used||   " , "||Number Of Instanc||   "
    for i in range(0,len(TopologyOnCluster)):
    	print "  ", TopologyOnCluster[i][0] ,"                ",TopologyOnCluster[i][1], "                 ",TopologyOnCluster[i][2], "                ", TopologyOnCluster[i][5], "                 ",TopologyOnCluster[i][6]

    print "=========================================================================================================="
    print "=========================================================================================================="
    print "||Machine ID||   " , "||Utilization Available||"
    for j in range(0,len(MachinesOfCluster)):
    	print "      " ,  MachinesOfCluster[j][0] , "                    " , MachinesOfCluster[j][1]

    #"""""""""""""""""""""""""""""end"""""""""""""""""""""""""""""""
    exit()
    '''

    return 1


#############################################################################################################################################

def SecondAssign():
    global ComponentList
    global StreamMap
    global DegreeInput
    global DegreeOutput
    global TaskDetailes
    global TopologyOnCluster
    global MachinesOfCluster
    '''
    MaxL =  -100

    for i in range(0,len(TopologyOnCluster)):
        TaskTemp = TopologyOnCluster[i][0]
        MachineTemp =  TopologyOnCluster[i][1]
        InputRateNow = TopologyOnCluster[i][2]
        for j in range(0,len(MachinesOfCluster)):
            if (MachinesOfCluster[j][0] == MachineTemp):
                UtilizationAvailable = MachinesOfCluster[j][1]

        UtilizationNow = TopologyOnCluster[i][5]
        CountOfInstance = TopologyOnCluster[i][6]
        Y =  TopologyOnCluster[i][4]
        UtilizationTotal = UtilizationNow + UtilizationAvailable
        MaxInputRate = int (float(UtilizationTotal * InputRateNow) / float (CountOfInstance * UtilizationNow * Y))

        Load = (float(InputRateNow) / float(MaxInputRate))
        if (Load > MaxL):
            MaxL = Load
            Task =  TaskTemp
            Machine = MachineTemp
    '''
    TempTopologyOnCluster = copy.deepcopy(TopologyOnCluster)
    TempMachinesOfCluster = copy.deepcopy(MachinesOfCluster)
    div = 1
    while div != 2048:
        Flag, XXTopologyOnCluster, YYMachinesOfCluster = IncreaseRateTopology(TempTopologyOnCluster,
                                                                              TempMachinesOfCluster, div)
        if Flag == -1:
            divInstance = 1
            while divInstance != 200:
                # print TempTopologyOnCluster
                # print "+++++++++++++++++++++++++++++++++++"
                # print "+++++++++++++++++++++++++++++++++++"
                # print XXTopologyOnCluster
                Finish, Flag1, XXTopologyOnCluster, YYMachinesOfCluster = NewInstanceFromTask(XXTopologyOnCluster,
                                                                                              YYMachinesOfCluster,
                                                                                              TempTopologyOnCluster,
                                                                                              TempMachinesOfCluster,
                                                                                            divInstance)
                # print TempMachinesOfCluster
                # print XXTopologyOnCluster
                if Finish == -1:
                    # div =  div * 2
                    divInstance += 1
                    #print YYMachinesOfCluster

                elif Flag1 == 1:
                    TempTopologyOnCluster = XXTopologyOnCluster
                    TempMachinesOfCluster = YYMachinesOfCluster
                    print divInstance
                    #exit()
                    break

            if (divInstance == 200):
                div = div * 2

        else:
            TempTopologyOnCluster = XXTopologyOnCluster
            TempMachinesOfCluster = YYMachinesOfCluster

    TopologyOnCluster = TempTopologyOnCluster
    MachinesOfCluster = TempMachinesOfCluster

    Throughput = 0

    print "||Task ID||   ", "||Run On Machine||   ", "||InputRate||   ", "||Utilization Used||   ", "||Number Of Instanc||   "
    for i in range(0, len(TopologyOnCluster)):
        Throughput += TopologyOnCluster[i][2] * TopologyOnCluster[i][6]
        print "  ", TopologyOnCluster[i][0], "                ", TopologyOnCluster[i][1], "                 ", \
        TopologyOnCluster[i][2], "                ", TopologyOnCluster[i][5], "                 ", TopologyOnCluster[i][
            6]
    print "=========================================================================================================="
    print "=========================================================================================================="
    print "||Machine ID||   ", "||Utilization Available||"
    for j in range(0, len(MachinesOfCluster)):
        print "      ", MachinesOfCluster[j][0], "                    ", MachinesOfCluster[j][1]

    print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    print " The maximum Throughput is : ", Throughput


def IncreaseRateTopology(TempTopologyOnCluster, TempMachinesOfCluster, div):
    global ComponentList
    global StreamMap
    global DegreeInput
    global DegreeOutput
    global TaskDetailes
    global TopologyOnCluster
    global MachinesOfCluster

    Temp_TopologyOnCluster = copy.deepcopy(TempTopologyOnCluster)
    Temp_MachinesOfCluster = copy.deepcopy(TempMachinesOfCluster)
    div = div
    Flag = 1

    for i in range(0, len(StreamMap)):
        Task = StreamMap[i][0]
        if DegreeInput[Task][1] == 0:
            NumberOfInstance = 0
            InputRateNow = 0
            for j in range(0, len(Temp_TopologyOnCluster)):
                if Task == Temp_TopologyOnCluster[j][0]:
                    InputRateNow = InputRateNow + (Temp_TopologyOnCluster[j][2] * Temp_TopologyOnCluster[j][6])
                    NumberOfInstance += Temp_TopologyOnCluster[j][6]
            Delta = int(InputRateNow / float(div))

            OutputRate = 0
            InputRateNow = InputRateNow + Delta
            InputRateNow = InputRateNow / NumberOfInstance
            for k in range(0, len(Temp_TopologyOnCluster)):
                if Task == Temp_TopologyOnCluster[k][0]:
                    InputRateNew = InputRateNow
                    UtilizationNow = Temp_TopologyOnCluster[k][5]
                    CountOfInstance = Temp_TopologyOnCluster[k][6]
                    Y = Temp_TopologyOnCluster[k][4]
                    InputRateOld = Temp_TopologyOnCluster[k][2]
                    UtilizationNew = (((InputRateNew - InputRateOld) / float(Y)) * CountOfInstance) + UtilizationNow

                    Temp_TopologyOnCluster[k][2] = InputRateNew
                    Temp_TopologyOnCluster[k][5] = UtilizationNew

                    alfa = Temp_TopologyOnCluster[k][3]

                    OutputRate += InputRateNew * alfa * CountOfInstance

            n = len(StreamMap[i]) - 1

            OutputRate = int(OutputRate / n)
            for l in range(1, len(StreamMap[i])):
                Successor = StreamMap[i][l]
                NumberOfInstance = 0

                for m in range(0, len(Temp_TopologyOnCluster)):
                    if Successor == Temp_TopologyOnCluster[m][0]:
                        NumberOfInstance += Temp_TopologyOnCluster[m][6]

                InputRateNew = int(OutputRate / float(NumberOfInstance))

                for m in range(0, len(Temp_TopologyOnCluster)):
                    if Successor == Temp_TopologyOnCluster[m][0]:
                        InputRateOld = Temp_TopologyOnCluster[m][2]
                        UtilizationOld = Temp_TopologyOnCluster[m][5]
                        CountOfInstance = Temp_TopologyOnCluster[m][6]
                        Y = Temp_TopologyOnCluster[m][4]

                        UtilizationNew = (((InputRateNew - InputRateOld) / float(Y)) * CountOfInstance) + UtilizationOld

                        Temp_TopologyOnCluster[m][2] = InputRateNew
                        Temp_TopologyOnCluster[m][5] = UtilizationNew

        elif DegreeOutput[Task][1] != 0:
            OutputRate = 0
            for j in range(0, len(Temp_TopologyOnCluster)):
                if Task == Temp_TopologyOnCluster[j][0]:
                    alfa = Temp_TopologyOnCluster[j][3]
                    CountOfInstance = Temp_TopologyOnCluster[j][6]
                    OutputRate += Temp_TopologyOnCluster[j][2] * alfa * CountOfInstance
            n = len(StreamMap[i]) - 1
            OutputRate = OutputRate / n

            for k in range(1, len(StreamMap[i])):
                Successor = StreamMap[i][k]
                NumberOfInstance = 0
                for l in range(0, len(Temp_TopologyOnCluster)):
                    if Successor == Temp_TopologyOnCluster[l][0]:
                        NumberOfInstance += Temp_TopologyOnCluster[l][6]

                InputRateNew = int(OutputRate / float(NumberOfInstance))

                for m in range(0, len(Temp_TopologyOnCluster)):
                    if Successor == Temp_TopologyOnCluster[m][0]:
                        InputRateOld = Temp_TopologyOnCluster[m][2]
                        UtilizationOld = Temp_TopologyOnCluster[m][5]
                        Y = Temp_TopologyOnCluster[m][4]
                        CountOfInstance = Temp_TopologyOnCluster[m][6]
                        UtilizationNew = (((InputRateNew - InputRateOld) / Y) * CountOfInstance) + UtilizationOld

                        Temp_TopologyOnCluster[m][2] = InputRateNew
                        Temp_TopologyOnCluster[m][5] = UtilizationNew

    for i in range(0, len(Temp_MachinesOfCluster)):
        UtilizationUsed = 0
        Machine = Temp_MachinesOfCluster[i][0]
        for j in range(0, len(Temp_TopologyOnCluster)):
            if Machine == Temp_TopologyOnCluster[j][1]:
                UtilizationUsed += Temp_TopologyOnCluster[j][5]
        if UtilizationUsed <= 100:
            Temp_MachinesOfCluster[i][1] = 100 - UtilizationUsed
        else:
            Temp_MachinesOfCluster[i][1] = 100 - UtilizationUsed
            Flag = -1

    return Flag, Temp_TopologyOnCluster, Temp_MachinesOfCluster


def NewInstanceFromTask(XXTopologyOnCluster, YYMachinesOfCluster, OldTopologyOnCluster, OldMachinesOfCluster,
                        divInstance):
    global TaskDetailes
    Flag = 1
    Finish = 1

    XTopologyOnCluster = copy.deepcopy(XXTopologyOnCluster)
    YMachinesOfCluster = copy.deepcopy(YYMachinesOfCluster)

    # YMachinesOfCluster = sorted(YMachinesOfCluster,key=lambda x: x[1], reverse=False)

    Max = -1000

    for i in range(0, len(YMachinesOfCluster)):
        Machine = YMachinesOfCluster[i][0]
        Utilization = YMachinesOfCluster[i][1]
        if Utilization < 0:
            for j in range(0, len(XTopologyOnCluster)):
                if Machine == XTopologyOnCluster[j][1]:
                    TempTask = XTopologyOnCluster[j][0]
                    InputRateNew = XTopologyOnCluster[j][2]
                    for k in range(0, len(OldTopologyOnCluster)):
                        # print TempTask
                        # print Machine
                        if OldTopologyOnCluster[k][0] == TempTask:
                            if OldTopologyOnCluster[k][1] == Machine:
                                for m in range(0, len(OldMachinesOfCluster)):
                                    if Machine == OldMachinesOfCluster[m][0]:
                                        UtilizatonAvalible = OldMachinesOfCluster[m][1]

                                InputRateOld = OldTopologyOnCluster[k][2]

                                CountOfInstance = OldTopologyOnCluster[k][6]

                                Y = OldTopologyOnCluster[k][4]

                                InputRateAvailable = int(
                                    (UtilizatonAvalible / float(CountOfInstance)) * Y) + InputRateOld
                                # print InputRateAvailable
                                # print InputRateNew

                                Load = (float(InputRateNew) / float(InputRateAvailable))
                                if Load > Max:
                                    Max = Load
                                    NewInstanceFromTask = TempTask
                                    # print TempTask

    InputRateTotal = 0
    NumberOfInstance = 0
    for k in range(0, len(XTopologyOnCluster)):
        if XTopologyOnCluster[k][0] == NewInstanceFromTask:
            CountOfInstance = XTopologyOnCluster[k][6]
            InputRateTotal += XTopologyOnCluster[k][2] * CountOfInstance
            NumberOfInstance += CountOfInstance

    NumberOfInstance += divInstance  # becacuse we want increase NumberOfInstace Of Task
    InputRateNew = int(InputRateTotal / float(NumberOfInstance))

    for m in range(0, len(XTopologyOnCluster)):
        if XTopologyOnCluster[m][0] == NewInstanceFromTask:
            CountOfInstance = XTopologyOnCluster[m][6]
            InputRateOld = XTopologyOnCluster[m][2]
            UtilizationOld = XTopologyOnCluster[m][5]
            Y = XTopologyOnCluster[m][4]
            UtilizationNew = (((InputRateNew - InputRateOld) / float(Y)) * CountOfInstance) + UtilizationOld

            XTopologyOnCluster[m][2] = InputRateNew
            XTopologyOnCluster[m][5] = UtilizationNew

    for l in range(0, len(YMachinesOfCluster)):
        MachineTemp = YMachinesOfCluster[l][0]
        UtilizationUsed = 0
        for n in range(0, len(XTopologyOnCluster)):
            if MachineTemp == XTopologyOnCluster[n][1]:
                UtilizationUsed += XTopologyOnCluster[n][5]
        if UtilizationUsed < 100:
            YMachinesOfCluster[l][1] = 100 - UtilizationUsed
        else:
            YMachinesOfCluster[l][1] = 100 - UtilizationUsed
            Flag = -1

    counter = 0
    while counter < divInstance:
        MachineCandid = []
        find = 0
        for k in range(0, len(TaskDetailes)):
            if NewInstanceFromTask == TaskDetailes[k][0]:
                InputRateDefault = TaskDetailes[k][2]
                YDeafualt = TaskDetailes[k][4]
                UtilizationDefault = TaskDetailes[k][5]
                UtilizationNeed = ((InputRateNew - InputRateDefault) / float(YDeafualt)) + UtilizationDefault

                MachineSelected = TaskDetailes[k][1]

                for f in range(0, len(YMachinesOfCluster)):
                    if MachineSelected == YMachinesOfCluster[f][0]:
                        if UtilizationNeed < YMachinesOfCluster[f][1]:
                            MachineCandid.append(
                                [NewInstanceFromTask, MachineSelected, UtilizationNeed, YMachinesOfCluster[f][1],
                                 TaskDetailes[k][3], TaskDetailes[k][4]])

        if len(MachineCandid) == 0:
            Finish = -1
            return Finish, Flag , XXTopologyOnCluster, YYMachinesOfCluster


        else:

            MachineCandid = sorted(MachineCandid, key=lambda x: x[5], reverse=True)

            for m in range(0, len(XTopologyOnCluster)):

                if ((MachineCandid[0][0] == XTopologyOnCluster[m][0]) & (
                    MachineCandid[0][1] == XTopologyOnCluster[m][1])):
                    XTopologyOnCluster[m][5] += MachineCandid[0][2]
                    XTopologyOnCluster[m][6] += 1
                    #print " ", "the task new instance ", MachineCandid[0][0]
                    #print " ", "the machine is selected ", MachineCandid[0][1]
                    counter += 1
                    find = 1
                    for x in range(0, len(YMachinesOfCluster)):
                        if YMachinesOfCluster[x][0] == MachineCandid[0][1]:
                            YMachinesOfCluster[x][1] = YMachinesOfCluster[x][1] - MachineCandid[0][2]
                            # return Finish , Flag , XTopologyOnCluster , YMachinesOfCluster

            if find == 0:
                #print " ", "the task new instance ", MachineCandid[0][0]
                #print " ", "the machine is selected ", MachineCandid[0][1]
                XTopologyOnCluster.append(
                    [MachineCandid[0][0], MachineCandid[0][1], InputRateNew, MachineCandid[0][4], MachineCandid[0][5],
                     MachineCandid[0][2], 1])
                counter += 1
                for x in range(0, len(YMachinesOfCluster)):
                    if YMachinesOfCluster[x][0] == MachineCandid[0][1]:
                        YMachinesOfCluster[x][1] = YMachinesOfCluster[x][1] - MachineCandid[0][2]
                        # return Finish , Flag , XTopologyOnCluster , YMachinesOfCluster
    return Finish, Flag, XTopologyOnCluster, YMachinesOfCluster


def main():
    if FirstAssign() == -1:
        print "can not execute"
        exit()

    SecondAssign()


main()