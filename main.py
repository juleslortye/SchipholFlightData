#############################################################
### CREATE FLIGHTS/FLIGHT SCHEDULE/PRESENCE PROBABILITIES ###
#############################################################

# Author: Jules L'Ortye (juleslortye@gmail.com)
#
# This script generates flight schedules and overview of arriving and departing flights
# Parameters to be set in the main method include:
#
#   - baseInputPath: path to input folder
#   - baseOutputPath = path to output folder. This folder should contain a 'FlightSchedules', 'Flights',
#     'Probabilities' and 'FlightStatistics' folder
#   - checkExistingFiles = boolean to indicate whether existing overviews can be used if available (decreases
#     computational time)
#   - computeProbDists = boolean to indicate whether the presence probabilities should be calculated based on
#     arrFlightList and depFlightList
#   - baseDate = base date which contains the year and the month, example: '2018-07-'
#   - dayRange = range of days that belong to the base date, for example: range(1, 32)
#   - minBucket = minimum size of the buckets used to compute probability distributions
#   - airlineMin = minimum of A/D movements per day for airline to be included in statistics in separate category
#
# Ensure proper credentials are set in the function getCredentials!
# Ensure that all packages as indicated under IMPORT PACKAGES are installed. In addition, install xlrd!
#
# Ensure that the proper input files are available in the input folder. These include:
#   InputAircraft.xlsx and InputAirport.xls


#######################
### IMPORT PACKAGES ###
#######################


import requests, sys, time, numpy as np, math, pandas, os
from datetime import datetime
from pandas import DataFrame
from collections import Counter


#########################
### SUPPORT FUNCTIONS ###
#########################


def getIntervals(dataRaw):
    flight = dataRaw[0]
    baseDate = dataRaw[1]

    startTimeRaw = "{}".format(flight['checkinAllocations']['checkinAllocations'][0][u'startTime'])
    endTimeRaw = "{}".format(flight['checkinAllocations']['checkinAllocations'][0][u'endTime'])
    flightTimeRaw = "{}".format(flight['scheduleTime'])[0:5]

    flightTime = datetime.strptime(baseDate + " " + flightTimeRaw, '%d-%m-%Y %H:%M')
    startTime = datetime.strptime(startTimeRaw[0:10] + " " + startTimeRaw[11:16], '%Y-%m-%d %H:%M')
    endTime = datetime.strptime(endTimeRaw[0:10] + " " + endTimeRaw[11:16], '%Y-%m-%d %H:%M')

    checkinIntervalSeconds = endTime - startTime
    departureIntervalSeconds = flightTime - endTime

    checkinIntervalMinutes = checkinIntervalSeconds.seconds / 60
    departureIntervalMinutes = departureIntervalSeconds.seconds / 60

    shortage = max(0, 40 - departureIntervalMinutes)

    checkinIntervalMinutes -= shortage
    departureIntervalMinutes += shortage

    return list([max(min(checkinIntervalMinutes,200),80), max(min(departureIntervalMinutes,80),40)])


def removeNoneFlights(grid):
    newGrid = []

    for i in range(0, len(grid)):
        newLine = grid[i][:]
        if '' not in newLine and 'None' not in newLine and None not in newLine:
            newGrid.append(newLine)

    return newGrid


def addRowsFlightList(FlightList, addFlightList):
    lenAddFlightList = len(addFlightList)

    for i in range(lenAddFlightList):
        FlightList.append(addFlightList[i][:])

    return FlightList


def createUniqueFlightList(FlightList, paramList):
    flightDirection = paramList[2]

    if FlightList == []:

        return []

    else:

        UniqueFlightList = [FlightList[0]]

        firstLine = FlightList[0][:]
        firstLineRef = getRefinedList(firstLine, flightDirection)

        UniqueCheckList = [firstLineRef]

        for i in range(1, len(FlightList)):

            checkLine = FlightList[i][:]
            checkLineRef = getRefinedList(checkLine, flightDirection)

            if checkLineRef not in UniqueCheckList:
                UniqueFlightList.append(FlightList[i])
                UniqueCheckList.append(checkLineRef)

        return UniqueFlightList


def getRefinedList(Line, flightDirection):
    if flightDirection == 'D':

        delFlightnumber = Line[1]
        delAirline = Line[6]
        delCodeshares = Line[9]
        delCheckinInterval = Line[11]
        delDepartureInterval = Line[12]

        Line.remove(delFlightnumber)
        Line.remove(delAirline)
        Line.remove(delCodeshares)
        Line.remove(delCheckinInterval)
        Line.remove(delDepartureInterval)

    else:

        delFlightnumber = Line[1]
        delAirline = Line[6]
        delCodeshares = Line[9]
        delBaggageClaim = Line[11]

        Line.remove(delFlightnumber)
        Line.remove(delAirline)
        Line.remove(delCodeshares)
        Line.remove(delBaggageClaim)

    return Line


def getFlightsHeaders():

    arrHeaders = list(['Rego', 'FlightNumber', 'STA', 'ATA', 'AC Type', 'Origin', 'Airline', 'Gate', 'Terminal', 'Codeshares',
         'TimeDiff', 'BaggageClaim'])

    depHeaders = list(['Rego', 'FlightNumber', 'STD', 'ATD', 'AC Type', 'Destination', 'Airline', 'Gate', 'Terminal', 'Codeshares',
         'TimeDiff', 'CheckinInterval', 'DepartureInterval'])

    return list([arrHeaders,depHeaders])


def getSkyTeamMembers():

    skyTeamMembers = list(['AFL','ARG','AMX','AEA','AFR','SMX','CAL','CSN','CSA','DAL','GIA','KQA','KLM','KAL','SVA','MEA','ROT','HVN','CXA'])

    return skyTeamMembers


def is2DList(checkList):
    if isinstance(checkList[0], list):
        return True
    else:
        return False


def makeList(checkList):
    if isinstance(checkList, list):
        return checkList
    else:
        return [checkList]


def getColumn(matrix, i):
    return [row[i] for row in matrix]


def getIndexMatch(haystack, needle):
    indices = [loc for loc, j in enumerate(haystack) if j == needle]
    return indices


def getElementsList(haystack, indices):
    newHaystack = []
    for index in indices:
        newHaystack = newHaystack + [haystack[index]]
    return newHaystack


def cleanAddClean(addCleanRawSorted):

    delRows = []

    for q in range(len(addCleanRawSorted) - 1):
        if addCleanRawSorted.iat[q + 1,4] == addCleanRawSorted.iat[q,4]:
            delRows.append(addCleanRawSorted.index[q])

    addClean = addCleanRawSorted.drop(delRows)

    return addClean


def getFlightIDS(flightsNoIn, flightsNoOut):

    if '[]' in flightsNoIn:
        flightsNoIn.remove('[]')

    if '[]' in flightsNoOut:
        flightsNoOut.remove('[]')


    inKL = makeList([s.startswith('KL') for s in flightsNoIn])
    outKL = makeList([s.startswith('KL') for s in flightsNoOut])

    if sum(inKL) > 0 and sum(outKL) > 0:

        # Both KL
        indexInKL = makeList([i for i, x in enumerate(inKL) if x])[0]
        indexOutKL = makeList([i for i, x in enumerate(outKL) if x])[0]

        flightNo = list([flightsNoIn[indexInKL], flightsNoOut[indexOutKL]])

    else:

        AirlinesIn = [x[0:2] for i, x in enumerate(flightsNoIn)]
        AirlinesOut = [x[0:2] for i, x in enumerate(flightsNoOut)]

        AirlinesBothAll = list(filter(lambda x: x in AirlinesIn, AirlinesOut))

        # No matching airlines
        if AirlinesBothAll == []:

            flightNo = list([flightsNoIn[0], flightsNoOut[0]])

        # matching airlines other than KL
        else:

            AirlinesBoth = AirlinesBothAll[0]

            inAirline = makeList([s.startswith(AirlinesBoth) for s in flightsNoIn])
            outAirline = makeList([s.startswith(AirlinesBoth) for s in flightsNoOut])

            indexInAirline = makeList([i for i, x in enumerate(inAirline) if x])[0]
            indexOutAirline = makeList([i for i, x in enumerate(outAirline) if x])[0]

            flightNo = list([flightsNoIn[indexInAirline], flightsNoOut[indexOutAirline]])

    return flightNo


def getRegions(baseInputPath,airportInOut):

    regionInOut = []
    emptyValueString = 'NS'

    AirportData = pandas.read_excel(baseInputPath + 'InputAirport.xls')
    Airports = AirportData["TNA_CODE_IATA"]

    for airport in airportInOut:
        indexAirport = Airports.index[Airports == airport].tolist()
        if indexAirport == []:
            RegionAirport = emptyValueString
        else:
            IndexAirportInt = indexAirport[0]
            RegionAirport = "{}".format(AirportData.iat[IndexAirportInt, 5])
        regionInOut.append(RegionAirport)

    return regionInOut


def enforceBounds(timeDiff,timeDim):

    emptystatement = ""

    if timeDiff < -timeDim:
        timeDiff = -timeDim
    elif timeDiff > timeDim-1:
        timeDiff = timeDim-1

    return timeDiff


def statsAirlineProcessor(statsAirline,airlineMin):

    newStatsAirline = DataFrame(columns = statsAirline.columns)
    otherStatsAirline = DataFrame(columns = statsAirline.columns)

    for index, row in statsAirline.iterrows():
        if sum(row)/len(row) >= airlineMin:
            newStatsAirline = newStatsAirline.append(row)
        else:
            otherStatsAirline = otherStatsAirline.append(row)

    otherLine = otherStatsAirline.sum(axis=0)
    otherLine = otherLine.rename("Other")
    otherLine = otherLine.astype('int64')
    newStatsAirline = newStatsAirline.append(otherLine)

    return newStatsAirline


######################
### MAIN FUNCTIONS ###
######################


def getData(paramList,page):
    url = "https://api.schiphol.nl/public-flights/flights"

    credentials = getCredentials()

    Qappid = credentials[0]
    Qappkey = credentials[1]
    Qscheduledate = paramList[0]
    Qscheduletime = paramList[1]
    Qflightdirection = paramList[2]

    querystring = {"app_id": Qappid, "app_key": Qappkey, "scheduledate": Qscheduledate, "scheduletime": Qscheduletime,
                   "flightdirection": Qflightdirection, "page": page}

    resourceversion = "v3"

    headers = {"resourceversion": resourceversion}

    departurePars = list(
        ['aircraftRegistration', 'flightName', 'scheduleTime', 'actualOffBlockTime', 'aircraftType', 'route',
         'prefixICAO', 'gate', 'terminal', 'codeshares', 'timeDiff', 'checkinAllocations'])
    arrivalPars = list(
        ['aircraftRegistration', 'flightName', 'scheduleTime', 'actualLandingTime', 'aircraftType', 'route',
         'prefixICAO', 'gate', 'terminal', 'codeshares', 'timeDiff', 'baggageClaim'])

    lenDepPars = len(departurePars)
    lenArrPars = len(arrivalPars)

    baseDate = Qscheduledate[8:10] + '-' + Qscheduledate[5:7] + '-' + Qscheduledate[0:4]

    try:

        response = requests.request("GET", url, headers=headers, params=querystring)

    except requests.exceptions.ConnectionError as error:

        print(error)
        sys.exit()

    if response.status_code == 200:

        flightList = response.json()
        flights = flightList["flights"]

        lenFlights = len(flights)

        if Qflightdirection == 'D':

            h, w = lenFlights, lenDepPars + 1
            result = [[None for x in range(w)] for y in range(h)]

            count = 0

            for flight in flights:

                flightServiceType = "{}".format(flight['serviceType'])

                if flightServiceType == 'J' or flightServiceType == 'C':

                    try:

                        for parNum in range(0, lenDepPars):

                            if departurePars[parNum] == 'aircraftType':
                                result[count][parNum] = "{}".format(flight['aircraftType']['iatasub'])
                            elif departurePars[parNum] == 'route':
                                result[count][parNum] = "{}".format(flight['route']['destinations'][0])
                            elif departurePars[parNum] == 'scheduleTime':
                                scheduleTime = "{}".format(flight['scheduleTime'])
                                result[count][parNum] = "{}".format(baseDate + " " + scheduleTime[0:5])
                            elif departurePars[parNum] == 'actualOffBlockTime':
                                AOBT = "{}".format(flight['actualOffBlockTime'])
                                result[count][parNum] = "{}".format(AOBT[8:10] + "-" + AOBT[5:7] + "-" + AOBT[0:4] + " " + AOBT[11:16])
                            elif departurePars[parNum] == 'codeshares':
                                try:
                                    result[count][parNum] = ["{}".format(s) for s in flight["codeshares"]["codeshares"]]
                                except Exception as e:
                                    result[count][parNum] = '[]'
                            elif departurePars[parNum] == 'timeDiff':
                                try:
                                    STD = flight["scheduleTime"]
                                    ATD = flight["actualOffBlockTime"]
                                    timeDiff = datetime.strptime(baseDate + " " + STD[0:5],'%d-%m-%Y %H:%M') - datetime.strptime(ATD[0:10] + " " + ATD[11:16], '%Y-%m-%d %H:%M')
                                    result[count][parNum] = int(-timeDiff.total_seconds() / 60)
                                except Exception as e:
                                    pass
                            elif departurePars[parNum] == 'checkinAllocations':
                                try:
                                    intervals = getIntervals(list([flight, baseDate]))
                                    result[count][parNum] = str(int(intervals[0]))
                                    result[count][parNum + 1] = str(int(intervals[1]))
                                except Exception as e:
                                    pass
                            else:
                                result[count][parNum] = "{}".format(flight[departurePars[parNum]])

                    except Exception as e:

                        print("Type Error: error in field other than codeshares, checkinAllocations and timeDiff")

                    count += 1

            result = result[0:count][:]

        elif Qflightdirection == 'A':

            h, w = lenFlights, lenArrPars
            result = [[None for x in range(w)] for y in range(h)]

            count = 0

            for flight in flights:

                flightServiceType = "{}".format(flight['serviceType'])

                if flightServiceType == 'J' or flightServiceType == 'C':

                    try:

                        for parNum in range(0, lenArrPars):

                            if arrivalPars[parNum] == 'aircraftType':
                                result[count][parNum] = "{}".format(flight['aircraftType']['iatasub'])
                            elif arrivalPars[parNum] == 'route':
                                result[count][parNum] = "{}".format(flight['route']['destinations'][0])
                            elif arrivalPars[parNum] == 'scheduleTime':
                                scheduleTime = "{}".format(flight['scheduleTime'])
                                result[count][parNum] = "{}".format(baseDate + " " + scheduleTime[0:5])
                            elif arrivalPars[parNum] == 'actualLandingTime':
                                ALT = "{}".format(flight['actualLandingTime'])
                                result[count][parNum] = "{}".format(ALT[8:10] + "-" + ALT[5:7] + "-" + ALT[0:4] + " " + ALT[11:16])
                            elif arrivalPars[parNum] == 'codeshares':
                                try:
                                    result[count][parNum] = ["{}".format(s) for s in flight["codeshares"]["codeshares"]]
                                except Exception as e:
                                    result[count][parNum] = '[]'
                            elif arrivalPars[parNum] == 'baggageClaim':
                                try:
                                    result[count][parNum] = "{}".format(flight['baggageClaim']['belts'][0])
                                except Exception as e:
                                    pass
                            elif arrivalPars[parNum] == 'timeDiff':
                                try:
                                    STA = flight["scheduleTime"]
                                    ATA = flight["actualLandingTime"]
                                    timeDiff = datetime.strptime(baseDate + " " + STA[0:5],'%d-%m-%Y %H:%M') - datetime.strptime(ATA[0:10] + " " + ATA[11:16], '%Y-%m-%d %H:%M')
                                    result[count][parNum] = int(-timeDiff.total_seconds() / 60)
                                except Exception as e:
                                    pass
                            else:
                                result[count][parNum] = "{}".format(flight[arrivalPars[parNum]])

                    except Exception as e:

                        print("Type Error: error in field other than codeshares, bagaggeClaim and timeDiff")

                    count += 1

            result = result[0:count][:]

        else:

            print("No valid departure/arrival code given")
            result = 'stop'

    else:

        result = 'stop'

    return result


def getFlightsDay(paramList):

    timePause = 60 / float(200) + 1

    maxFlightsDay = 6000
    maxQueries = int(maxFlightsDay/20)

    FlightList = []

    for page in range(maxQueries):
        addFlightListRaw = getData(paramList,page)
        if isinstance(addFlightListRaw,list):
            addFlightList = removeNoneFlights(addFlightListRaw)
            FlightList = addRowsFlightList(FlightList, addFlightList)
            time.sleep(timePause)
        else:
            break

    UniqueFlightList = createUniqueFlightList(FlightList, paramList)

    return UniqueFlightList


def getFlightListFromAPI(baseDate, dayRange, flightDirection):

    FlightList = []

    for i in dayRange:

        t = time.time()

        if i < 10:
            date = baseDate + '0' + str(i)
        else:
            date = baseDate + str(i)

        paramList = list([date, '00:00', flightDirection])
        try:
            DayFlightList = getFlightsDay(paramList)
        except Exception as e:
            print(e)
        else:
            FlightList = addRowsFlightList(FlightList, DayFlightList)

        elapsed = time.time() - t
        progressIndicator = flightDirection + ": " + str(i) + "/" + str(max(dayRange)) + " in " + str(math.ceil((elapsed/60)*100)/100) + " minutes"
        print(progressIndicator)

        time.sleep(30)

    return FlightList


def getFlightSchedule(baseInputPath,baseDate, arrFlightList, depFlightList):

    t = time.time()

    uniqueRegos = list(set(getColumn(arrFlightList, 0) + getColumn(depFlightList, 0)))
    FlightSchedulePerACDay = []

    for rego in uniqueRegos:

        arrFlights = makeList(getIndexMatch(getColumn(arrFlightList, 0), rego))
        arrFlightsDay = getElementsList(getColumn(arrFlightList, 2), arrFlights)
        arrSize = (len(arrFlightsDay), 5)
        arrFlightsDayClean = np.zeros(arrSize)

        for j in range(len(arrFlightsDay)):
            arrFlight = arrFlightsDay[j]
            arrFlightMinutes = arrFlight[-2:]
            arrFlightHours = arrFlight[-5:-3]
            arrFlightDay = arrFlight[0:2]

            arrFlightsDayClean[j, :] = [arrFlights[j], int(arrFlightDay), int(arrFlightHours), int(arrFlightMinutes), 0]

        arrFlightsDayClean = arrFlightsDayClean.astype(int)

        depFlights = makeList(getIndexMatch(getColumn(depFlightList, 0), rego))
        depFlightsDay = getElementsList(getColumn(depFlightList, 2), depFlights)

        depSize = (len(depFlightsDay), 5)
        depFlightsDayClean = np.zeros(depSize)

        for j in range(len(depFlightsDay)):
            depFlight = depFlightsDay[j]
            depFlightMinutes = depFlight[-2:]
            depFlightHours = depFlight[-5:-3]
            depFlightDay = depFlight[0:2]

            depFlightsDayClean[j, :] = [depFlights[j], int(depFlightDay), int(depFlightHours), int(depFlightMinutes), 1]

        depFlightsDayClean = depFlightsDayClean.astype(int)

        addCleanRaw = DataFrame(np.concatenate((arrFlightsDayClean, depFlightsDayClean), axis=0),columns=['a', 'b', 'c', 'd', 'e'])
        addCleanRawSorted = addCleanRaw.sort_values(['b', 'c', 'd'], ascending=[True, True, True])

        addClean = cleanAddClean(addCleanRawSorted)

        days = list(set(addClean.iloc[:, 1].copy()))

        emptyValueString = 'NS'
        emptyValueInteger = 0

        for day in days:

            if day < 10:
                dayString = "0" + str(day)
            else:
                dayString = str(day)

            ns = 0
            flightsPerDay = addClean.iloc[[i for i, x in enumerate(list(addClean.iloc[:, 1] == day)) if x], :]

            lenFlightsPerDay = len(flightsPerDay)
            movementsPerDay = lenFlightsPerDay

            if flightsPerDay.iat[0, 4] == 1:
                movementsPerDay = movementsPerDay - 1

                Date = day
                AC_reg = rego
                ID_in = emptyValueString
                ID_out = depFlightList[flightsPerDay.iat[0, 0]][1]
                STAnum = emptyValueInteger  # set for sorting
                STDnum = 100 * flightsPerDay.iat[0, 2] + flightsPerDay.iat[0, 3]
                STA = emptyValueString
                STD = depFlightList[flightsPerDay.iat[0, 0]][2]
                ATA = emptyValueString
                ATD = depFlightList[flightsPerDay.iat[0, 0]][3]
                AC_type = depFlightList[flightsPerDay.iat[0, 0]][4]
                Origin = emptyValueString
                Destination = depFlightList[flightsPerDay.iat[0, 0]][5]
                Operator = depFlightList[flightsPerDay.iat[0, 0]][6]
                GateGroupIn = emptyValueString
                GateGroupOut = depFlightList[flightsPerDay.iat[0, 0]][7][0] + '-pier'
                GateIn = emptyValueString
                GateOut = depFlightList[flightsPerDay.iat[0, 0]][7]
                TerminalIn = emptyValueInteger
                TerminalOut = str(int(depFlightList[flightsPerDay.iat[0, 0]][8]))
                BaggageBelt = emptyValueString
                CII = depFlightList[flightsPerDay.iat[0, 0]][11]
                CheckInInterval = int(CII) if CII is not None else emptyValueInteger
                DI = depFlightList[flightsPerDay.iat[0, 0]][12]
                DepartureInterval = int(DI) if DI is not None else emptyValueInteger
                ADI = datetime.strptime(STD, '%d-%m-%Y %H:%M') - datetime.strptime(baseDate + dayString + " 00:00", '%Y-%m-%d %H:%M')
                ArrDepInterval = int(ADI.seconds / 60)

                Line = [Date, AC_reg, ID_in, ID_out, STAnum, STDnum, STA, STD, ATA, ATD, AC_type, Origin,
                        Destination, Operator, GateGroupIn, GateGroupOut, GateIn, GateOut, TerminalIn, TerminalOut,
                        BaggageBelt, CheckInInterval, DepartureInterval,ArrDepInterval]
                FlightSchedulePerACDay.append(Line)
                ns = 1

            if movementsPerDay > 1:

                for n in range(0, int(math.floor(movementsPerDay / 2))):

                    if flightsPerDay.iat[ns, 4] == 0:
                        offsetArr = 0
                        offsetDep = 1
                    else:
                        offsetArr = 1
                        offsetDep = 0

                    arrNo = n*2 + offsetArr + ns
                    depNo = n*2 + offsetDep + ns

                    codeSharesInRaw = arrFlightList[flightsPerDay.iat[arrNo, 0]][9]
                    if isinstance(codeSharesInRaw, list):
                        codesharesIn = makeList(codeSharesInRaw)
                    else:
                        codesharesIn = makeList(eval(codeSharesInRaw))

                    codeSharesOutRaw = depFlightList[flightsPerDay.iat[depNo, 0]][9]
                    if isinstance(codeSharesOutRaw, list):
                        codesharesOut = makeList(codeSharesOutRaw)
                    else:
                        codesharesOut = makeList(eval(codeSharesOutRaw))

                    flightIn = makeList(arrFlightList[flightsPerDay.iat[arrNo, 0]][1])
                    flightOut = makeList(depFlightList[flightsPerDay.iat[depNo, 0]][1])

                    flightsNoIn = list(set(codesharesIn + flightIn))
                    flightsNoOut = list(set(codesharesOut + flightOut))

                    IDs = getFlightIDS(flightsNoIn,flightsNoOut)

                    Date = day
                    AC_reg = rego
                    ID_in = IDs[0]
                    ID_out = IDs[1]
                    STAnum = 100 * flightsPerDay.iat[arrNo, 2] + flightsPerDay.iat[arrNo, 3]
                    STDnum = 100 * flightsPerDay.iat[depNo, 2] + flightsPerDay.iat[depNo, 3]
                    STA = arrFlightList[flightsPerDay.iat[arrNo, 0]][2]
                    STD = depFlightList[flightsPerDay.iat[depNo, 0]][2]
                    ATA = arrFlightList[flightsPerDay.iat[arrNo, 0]][3]
                    ATD = depFlightList[flightsPerDay.iat[depNo, 0]][3]
                    AC_type = depFlightList[flightsPerDay.iat[depNo, 0]][4]
                    Origin = arrFlightList[flightsPerDay.iat[arrNo, 0]][5]
                    Destination = depFlightList[flightsPerDay.iat[depNo, 0]][5]
                    Operator = depFlightList[flightsPerDay.iat[depNo, 0]][6]
                    GateGroupIn = arrFlightList[flightsPerDay.iat[arrNo, 0]][7][0] + '-pier'  # arr
                    GateGroupOut = depFlightList[flightsPerDay.iat[depNo, 0]][7][0] + '-pier'
                    GateIn = arrFlightList[flightsPerDay.iat[arrNo, 0]][7]  # arr
                    GateOut = depFlightList[flightsPerDay.iat[depNo, 0]][7]
                    TerminalIn = str(int(arrFlightList[flightsPerDay.iat[arrNo, 0]][8]))
                    TerminalOut = str(int(depFlightList[flightsPerDay.iat[depNo, 0]][8]))
                    BBval = arrFlightList[flightsPerDay.iat[arrNo, 0]][11]
                    BaggageBelt = BBval if BBval is not None else emptyValueString
                    CII = depFlightList[flightsPerDay.iat[depNo, 0]][11]
                    CheckInInterval = int(CII) if CII is not None else emptyValueInteger
                    DI = depFlightList[flightsPerDay.iat[depNo, 0]][12]
                    DepartureInterval = int(DI) if DI is not None else emptyValueInteger
                    ADI = datetime.strptime(STD, '%d-%m-%Y %H:%M') - datetime.strptime(STA, '%d-%m-%Y %H:%M')
                    ArrDepInterval = int(ADI.seconds/60)

                    Line = [Date, AC_reg, ID_in, ID_out, STAnum, STDnum, STA, STD, ATA, ATD, AC_type, Origin,
                            Destination, Operator, GateGroupIn, GateGroupOut, GateIn, GateOut, TerminalIn, TerminalOut,
                            BaggageBelt, CheckInInterval, DepartureInterval,ArrDepInterval]
                    FlightSchedulePerACDay.append(Line)

            if flightsPerDay.iat[lenFlightsPerDay - 1, 4] == 0:
                finalIndex = lenFlightsPerDay - 1

                Date = day
                AC_reg = rego
                ID_in = arrFlightList[flightsPerDay.iat[finalIndex, 0]][1]
                ID_out = emptyValueString
                STAnum = 100 * flightsPerDay.iat[finalIndex, 2] + flightsPerDay.iat[finalIndex, 3]
                STDnum = 2359  # set for sorting
                STA = arrFlightList[flightsPerDay.iat[finalIndex, 0]][2]
                STD = emptyValueString
                ATA = arrFlightList[flightsPerDay.iat[finalIndex, 0]][3]
                ATD = emptyValueString
                AC_type = arrFlightList[flightsPerDay.iat[finalIndex, 0]][4]
                Origin = arrFlightList[flightsPerDay.iat[finalIndex, 0]][5]
                Destination = emptyValueString
                Operator = arrFlightList[flightsPerDay.iat[finalIndex, 0]][6]
                GateGroupIn = arrFlightList[flightsPerDay.iat[finalIndex, 0]][7][0] + '-pier'
                GateGroupOut = emptyValueString
                GateIn = arrFlightList[flightsPerDay.iat[finalIndex, 0]][7]
                GateOut = emptyValueString
                TerminalIn = str(int(arrFlightList[flightsPerDay.iat[finalIndex, 0]][8]))
                TerminalOut = emptyValueInteger
                BBval = arrFlightList[flightsPerDay.iat[finalIndex, 0]][11]
                BaggageBelt = BBval if BBval != '0' else emptyValueString
                CheckInInterval = emptyValueInteger
                DepartureInterval = emptyValueInteger
                ADI = datetime.strptime(baseDate + dayString + " 23:59", '%Y-%m-%d %H:%M') - datetime.strptime(STA, '%d-%m-%Y %H:%M')
                ArrDepInterval = int(ADI.seconds / 60)

                Line = [Date, AC_reg, ID_in, ID_out, STAnum, STDnum, STA, STD, ATA, ATD, AC_type, Origin,
                        Destination, Operator, GateGroupIn, GateGroupOut, GateIn, GateOut, TerminalIn, TerminalOut,
                        BaggageBelt, CheckInInterval, DepartureInterval,ArrDepInterval]
                FlightSchedulePerACDay.append(Line)

    headers = ["Date", "AC_reg", "ID_in", "ID_out", "STAnum", "STDnum", "STA", "STD", "ATA", "ATD", "AC_type", "Origin",
               "Destination", "Operator", "GateGroupIn", "GateGroupOut", "GateIn", "GateOut", "TerminalIn", "TerminalOut",
               "BaggageBelt", "CheckInInterval", "DepartureInterval","ArrDepInterval"]
    FlightSchedule = DataFrame(FlightSchedulePerACDay, columns=headers)

    FlightSchedule['STAnum'].astype('int')
    FlightSchedule['STDnum'].astype('int')
    FlightSchedule['ArrDepInterval'].astype('int')

    FlightScheduleSorted = FlightSchedule.sort_values(['Date', 'STAnum', 'STDnum', 'Operator'],
                                                      ascending=[True, True, True, True])

    FlightScheduleComplete = enrichFlightSchedule(FlightScheduleSorted, baseInputPath)

    orderColumns = ["ID_in", "ID_out", "STAnum", "STDnum", "AC_Size", "Customs_In", "Customs_Out", "Operator", "Origin",
                    "Destination", "AC_type", "AC_reg", "Region_In", "Region_Out", "Date", "GateGroupIn", "GateGroupOut",
                    "GateIn", "GateOut", "STA", "STD", "ATA", "ATD", "TerminalIn", "TerminalOut", "BaggageBelt",
                     "CheckInInterval", "DepartureInterval","ArrDepInterval"]

    FlightScheduleColumnCheck = FlightScheduleComplete[orderColumns]

    FlightScheduleDoubleSort = FlightScheduleColumnCheck.sort_values(['Date', "ID_in", "ID_out", 'STDnum', "STAnum"],
                                                      ascending=[True, True, True, True, True])

    FlightScheduleCleaned = FlightScheduleDoubleSort[FlightScheduleDoubleSort.ArrDepInterval >= 40]

    elapsed = time.time() - t
    progressIndicator = "F: 1/1 in " + str(math.ceil((elapsed/60)*100)/100) + " minutes"
    print(progressIndicator)

    return FlightScheduleCleaned


def enrichFlightSchedule(FlightScheduleBare, baseInputPath):
    ACSizeData = pandas.read_excel(baseInputPath + 'InputAircraft.xlsx')
    ACTypes = [str(i) for i in ACSizeData['TYPE'].values.tolist()]

    AirportData = pandas.read_excel(baseInputPath + 'InputAirport.xls')
    Airports = AirportData["TNA_CODE_IATA"]

    lenFSSB = len(FlightScheduleBare)

    addData = []

    NotAvailableAC = []

    emptyValueString = 'NS'
    emptyValueInteger = 0

    for i in range(lenFSSB):

        if FlightScheduleBare.iat[i, 10] in ACTypes:
            indexAC = makeList(ACTypes.index(FlightScheduleBare.iat[i, 10]))
        else:
            indexAC = []

        if indexAC == []:
            sizeAC = 4
            NotAvailableAC.append(FlightScheduleBare.iat[i, 10])
        else:
            sizeAC = ACSizeData.iat[indexAC[0], 1]

        origin = FlightScheduleBare.iat[i, 11]

        if origin == emptyValueString:
            RegionOrg = emptyValueString
            CustomsOrg = emptyValueInteger
        else:
            indexAirport = Airports.index[Airports == origin].tolist()

            if indexAirport == []:
                RegionOrg = emptyValueString
                CustomsOrg = emptyValueInteger
            else:
                IndexAirportInt = indexAirport[0]
                RegionOrg = "{}".format(AirportData.iat[IndexAirportInt, 5])
                EU = "{}".format(AirportData.iat[IndexAirportInt, 18])
                ER = "{}".format(AirportData.iat[IndexAirportInt, 17])
                US = "{}".format(AirportData.iat[IndexAirportInt, 19])

                if EU == 'Y':
                    CustomsOrg = '1'
                elif ER == 'Y':
                    CustomsOrg = '2'
                elif US == 'Y':
                    CustomsOrg = '3'
                else:
                    CustomsOrg = '3'

        destination = FlightScheduleBare.iat[i, 12]

        if destination == emptyValueString:
            RegionDest = emptyValueString
            CustomsDest = emptyValueInteger
        else:
            indexAirport = Airports.index[Airports == destination].tolist()

            if indexAirport == []:
                RegionDest = emptyValueString
                CustomsDest = emptyValueInteger
            else:
                IndexAirportInt = indexAirport[0]
                RegionDest = "{}".format(AirportData.iat[IndexAirportInt, 5])
                EU = "{}".format(AirportData.iat[IndexAirportInt, 18])
                ER = "{}".format(AirportData.iat[IndexAirportInt, 17])
                US = "{}".format(AirportData.iat[IndexAirportInt, 19])

                if EU == 'Y':
                    CustomsDest = '1'
                elif ER == 'Y':
                    CustomsDest = '2'
                elif US == 'Y':
                    CustomsDest = '3'
                else:
                    CustomsDest = '3'

        Line = [sizeAC, RegionOrg, RegionDest, CustomsOrg, CustomsDest]
        addData.append(Line)

    headers = ['AC_Size', 'Region_In', 'Region_Out', 'Customs_In', 'Customs_Out']
    newData = DataFrame(addData, columns=headers)

    newData['AC_Size'].astype('int')

    FlightScheduleBareIndexed = FlightScheduleBare.reset_index().drop(['index'], axis=1)

    FlightSchedule = pandas.concat([FlightScheduleBareIndexed, newData], axis=1)

    return FlightSchedule


def writeToCSV(FlightSchedule,ProbDists,Statistics,arrFlightList,depFlightList,baseDate,dayRange,baseOutputPath):

    t = time.time()
    status = True

    try:

        headers = getFlightsHeaders()

        arrHeaders = headers[0]
        fileNameArr = baseOutputPath + 'Flights/ArrivingFlights_' + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv'

        arrivals = DataFrame(arrFlightList, columns=arrHeaders)
        arrivals.to_csv(fileNameArr)

        depHeaders = headers[1]
        fileNameDep = baseOutputPath + 'Flights/DepartingFlights_' + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv'

        departures = DataFrame(depFlightList, columns=depHeaders)
        departures.to_csv(fileNameDep)

        baseFileNameStats = baseOutputPath + 'FlightStatistics/'
        baseFileNameAirlines = baseOutputPath + 'Airlines/'

        statsRegionIn = Statistics[0]
        statsRegionOut = Statistics[1]
        statsAirlineIn = Statistics[2]
        statsAirlineOut = Statistics[3]
        transferAirlines = Statistics[4]

        statsRegionIn.to_csv(baseFileNameStats + "statsRegionIn" + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv')
        statsRegionOut.to_csv(baseFileNameStats + "statsRegionOut" + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv')
        statsAirlineIn.to_csv(baseFileNameStats + "statsAirlineIn" + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv')
        statsAirlineOut.to_csv(baseFileNameStats + "statsAirlineOut" + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv')
        transferAirlines.to_csv(baseFileNameAirlines + "transferAirlines.csv")

        if ProbDists != "":

            baseFileName = baseOutputPath + 'Probabilities/'

            airlineInNames = DataFrame(ProbDists[0])
            airlineOutNames = DataFrame(ProbDists[1])
            regionInNames = DataFrame(ProbDists[2])
            regionOutNames = DataFrame(ProbDists[3])
            airlineInDists = DataFrame(ProbDists[4])
            airlineOutDists = DataFrame(ProbDists[5])
            regionInDists = DataFrame(ProbDists[6])
            regionOutDists = DataFrame(ProbDists[7])
            inDist = DataFrame(ProbDists[8])
            inDist = inDist.T
            outDist = DataFrame(ProbDists[9])
            outDist = outDist.T


            airlineInNames.to_csv(baseFileName + "airlineInNames" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            airlineOutNames.to_csv(baseFileName + "airlineOutNames" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            regionInNames.to_csv(baseFileName + "regionInNames" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            regionOutNames.to_csv(baseFileName + "regionOutNames" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            airlineInDists.to_csv(baseFileName + "airlineInDists" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            airlineOutDists.to_csv(baseFileName + "airlineOutDists" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            regionInDists.to_csv(baseFileName + "regionInDists" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            regionOutDists.to_csv(baseFileName + "regionOutDists" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            inDist.to_csv(baseFileName + "inDist" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)
            outDist.to_csv(baseFileName + "outDist" + "_" + str(len(dayRange)) + "D.csv", header=False, index=False)

        for day in dayRange:

            dayFlightSchedule = FlightSchedule.loc[FlightSchedule['Date'] == day].reset_index().drop(['index'], axis=1)
            fileNameFS = baseOutputPath + 'FlightSchedules/FlightSchedule_' + baseDate + str(day) + '.csv'

            dayFlightSchedule.to_csv(fileNameFS)

    except requests.exceptions.ConnectionError as error:

        print(error)
        status = False

    elapsed = time.time() - t
    progressIndicator = "W: 1/1 in " + str(math.ceil((elapsed/60)*100)/100) + " minutes"
    print(progressIndicator)

    return status


def getFlightList(baseDate, dayRange, flightDirection, baseOutputPath, checkExistingFiles):


    if checkExistingFiles:

        if flightDirection == 'A':

            print("Start gathering arriving flight list")

            fileName = baseOutputPath + 'Flights/ArrivingFlights_' + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv'

        else:

            print("Start gathering departing flight list")

            fileName = baseOutputPath + 'Flights/DepartingFlights_' + baseDate + "(" + str(min(dayRange)) + "-" + str(max(dayRange)) + ').csv'

        if os.path.isfile(fileName):

            FlightList = pandas.read_csv(fileName, index_col=0).values.tolist()

        else:

            FlightList = getFlightListFromAPI(baseDate, dayRange, flightDirection)

    else:

        FlightList = getFlightListFromAPI(baseDate, dayRange, flightDirection)


    return FlightList


def getProbabilityDistributions(arrFlightList,depFlightList,baseInputPath,minBucket,computeProbDists):

    t = time.time()
    headers = getFlightsHeaders()

    if computeProbDists:

        timeDim = 60 * 24 * 2

        arrFlights = DataFrame(arrFlightList, columns=headers[0])
        depFlights = DataFrame(depFlightList, columns=headers[1])

        airlineIn = list(arrFlights.get('Airline'))
        airlineOut = list(depFlights.get('Airline'))

        dictAirlineIn = Counter(airlineIn)
        dictAirlineOut = Counter(airlineOut)

        airportIn = list(arrFlights.get('Origin'))
        airportOut = list(depFlights.get('Destination'))

        regionIn = getRegions(baseInputPath, airportIn)
        regionOut = getRegions(baseInputPath, airportOut)

        dictRegionIn = Counter(regionIn)
        dictRegionOut = Counter(regionOut)

        arrFlightsComp = pandas.concat([arrFlights, pandas.DataFrame({'Region':regionIn})], axis=1)
        depFlightsComp = pandas.concat([depFlights, pandas.DataFrame({'Region':regionOut})], axis=1)

        airlineInNames = []
        airlineInDists = np.empty([0,timeDim])

        for key in sorted(dictAirlineIn.keys()):
            if dictAirlineIn.get(key) >= minBucket:
                airlineInNames.append(key)
                emptyCountLine = np.zeros([1,timeDim])
                arrList = arrFlightsComp[arrFlightsComp.Airline == key]
                TimeDiffs = arrList.get("TimeDiff")
                for timeDiff in TimeDiffs:
                    emptyCountLine[0,int(timeDim/2 + enforceBounds(timeDiff,timeDim/2))] += 1
                distribution = np.cumsum(emptyCountLine)/emptyCountLine.sum()
                airlineInDists = np.append(airlineInDists,distribution.reshape(1,timeDim),axis=0)

        airlineOutNames = []
        airlineOutDists = np.empty([0,timeDim])

        for key in sorted(dictAirlineOut.keys()):
            if dictAirlineOut.get(key) >= minBucket:
                airlineOutNames.append(key)
                emptyCountLine = np.zeros([1,timeDim])
                depList = depFlightsComp[depFlightsComp.Airline == key]
                TimeDiffs = depList.get("TimeDiff")
                for timeDiff in TimeDiffs:
                    emptyCountLine[0,int(timeDim/2 + enforceBounds(timeDiff,timeDim/2))] += 1
                distribution = np.cumsum(emptyCountLine)/emptyCountLine.sum()
                airlineOutDists = np.append(airlineOutDists,distribution.reshape(1,timeDim),axis=0)
        airlineOutDists = (airlineOutDists - 1) * -1

        regionInNames = []
        regionInDists = np.empty([0,timeDim])

        for key in sorted(dictRegionIn.keys()):
            if dictRegionIn.get(key) >= minBucket:
                regionInNames.append(key)
                emptyCountLine = np.zeros([1,timeDim])
                arrList = arrFlightsComp[arrFlightsComp.Region == key]
                TimeDiffs = arrList.get("TimeDiff")
                for timeDiff in TimeDiffs:
                    emptyCountLine[0,int(timeDim/2 + enforceBounds(timeDiff,timeDim/2))] += 1
                distribution = np.cumsum(emptyCountLine)/emptyCountLine.sum()
                regionInDists = np.append(regionInDists,distribution.reshape(1,timeDim),axis=0)

        regionOutNames = []
        regionOutDists = np.empty([0, timeDim])

        for key in sorted(dictRegionOut.keys()):
            if dictRegionOut.get(key) >= minBucket:
                regionOutNames.append(key)
                emptyCountLine = np.zeros([1, timeDim])
                depList = depFlightsComp[depFlightsComp.Region == key]
                TimeDiffs = depList.get("TimeDiff")
                for timeDiff in TimeDiffs:
                    emptyCountLine[0, int(timeDim/2 + enforceBounds(timeDiff,timeDim/2))] += 1
                distribution = np.cumsum(emptyCountLine) / emptyCountLine.sum()
                regionOutDists = np.append(regionOutDists, distribution.reshape(1, timeDim), axis=0)
        regionOutDists = (regionOutDists-1)*-1


        TimeDiffs = arrFlightsComp.get("TimeDiff")
        emptyLine = np.zeros([1, timeDim])
        for timeDiff in TimeDiffs:
            emptyLine[0, int(timeDim/2 + enforceBounds(timeDiff, timeDim / 2))] += 1
        inDist = np.cumsum(emptyCountLine) / emptyCountLine.sum()

        TimeDiffs = depFlightsComp.get("TimeDiff")
        emptyLine = np.zeros([1, timeDim])
        for timeDiff in TimeDiffs:
            emptyLine[0, int(timeDim/2 + enforceBounds(timeDiff, timeDim / 2))] += 1
        outDist = np.cumsum(emptyCountLine) / emptyCountLine.sum()
        outDist = (outDist-1)*-1

        returnValue = list([airlineInNames,airlineOutNames,regionInNames,regionOutNames,airlineInDists,airlineOutDists,regionInDists,regionOutDists,inDist,outDist])

    else:

        returnValue = ""

    elapsed = time.time() - t
    progressIndicator = "P: 1/1 in " + str(math.ceil((elapsed/60)*100)/100) + " minutes"
    print(progressIndicator)

    return returnValue


def getStatistics(arrFlightList,depFlightList,dayRange,airlineMin):

    t = time.time()

    uniqueDays = list(dayRange)

    headers = getFlightsHeaders()

    arrFlights = DataFrame(arrFlightList, columns=headers[0])
    depFlights = DataFrame(depFlightList, columns=headers[1])

    airportIn = list(arrFlights.get('Origin'))
    airportOut = list(depFlights.get('Destination'))

    regionIn = getRegions(baseInputPath, airportIn)
    regionOut = getRegions(baseInputPath, airportOut)

    arrFlightsComp = pandas.concat([arrFlights, pandas.DataFrame({'Region': regionIn})], axis=1)
    depFlightsComp = pandas.concat([depFlights, pandas.DataFrame({'Region': regionOut})], axis=1)

    uniqueRegionIn = sorted(list(set(regionIn)))
    uniqueRegionOut = sorted(list(set(regionOut)))

    uniqueAirlineIn = sorted(list(set(arrFlights.get('Airline'))))
    uniqueAirlineOut = sorted(list(set(depFlights.get('Airline'))))

    statsRegionIn = pandas.DataFrame(columns=uniqueDays,index=uniqueRegionIn)
    statsRegionOut = pandas.DataFrame(columns=uniqueDays,index=uniqueRegionOut)

    statsAirlineIn = pandas.DataFrame(columns=uniqueDays,index=uniqueAirlineIn)
    statsAirlineOut = pandas.DataFrame(columns=uniqueDays,index=uniqueAirlineOut)

    for column in dayRange:
        if column < 10:
            dayColumn = "0" + str(column)
        else:
            dayColumn = str(column)
        dayArrFlightsComp = arrFlightsComp[arrFlightsComp.STA.apply(lambda x: x[0:2]) == dayColumn]
        dayDepFlightsComp = depFlightsComp[depFlightsComp.STD.apply(lambda x: x[0:2]) == dayColumn]
        for rowInReg in uniqueRegionIn:
            statsRegionIn.at[rowInReg,column] = len(dayArrFlightsComp[dayArrFlightsComp.Region == rowInReg])
        for rowOutReg in uniqueRegionOut:
            statsRegionOut.at[rowOutReg,column] = len(dayDepFlightsComp[dayDepFlightsComp.Region == rowOutReg])
        for rowInAir in uniqueAirlineIn:
            statsAirlineIn.at[rowInAir,column] = len(dayArrFlightsComp[dayArrFlightsComp.Airline == rowInAir])
        for rowOutAir in uniqueAirlineOut:
            statsAirlineOut.at[rowOutAir,column] = len(dayDepFlightsComp[dayDepFlightsComp.Airline == rowOutAir])

    statsAirlineIn = statsAirlineProcessor(statsAirlineIn,airlineMin)
    statsAirlineOut = statsAirlineProcessor(statsAirlineOut,airlineMin)

    allAirlinesRaw = sorted(list(set(uniqueAirlineIn + uniqueAirlineOut)))
    skyTeamMembers = getSkyTeamMembers()
    skInd = [0] * len(allAirlinesRaw)

    for i, x in enumerate(allAirlinesRaw):
        if x in skyTeamMembers:
            skInd[i] = 1

    transferAirlines = pandas.DataFrame({'Airline': allAirlinesRaw, 'Transfer': skInd})

    elapsed = time.time() - t
    progressIndicator = "S: 1/1 in " + str(math.ceil((elapsed / 60) * 100) / 100) + " minutes"
    print(progressIndicator)

    return list([statsRegionIn,statsRegionOut,statsAirlineIn,statsAirlineOut,transferAirlines])


#######################
### API CREDENTIALS ###
#######################


def getCredentials():

    appID = "XXXX"
    appKEY = "XXXX"

    return list([appID,appKEY])


###################
### MAIN SCRIPT ###
###################


if __name__ == "__main__":

    baseInputPath = '/Users/juleslortye/Dropbox/TU Delft Thesis/Python/FlightData/Input/'
    baseOutputPath = '/Users/juleslortye/Dropbox/TU Delft Thesis/Python/FlightData/Output/'

    checkExistingFiles = True
    computeProbDists = True
    baseDate = '2018-07-'
    dayRange = range(1,31)
    minBucket = 200
    airlineMin = 25

    arrFlightList = getFlightList(baseDate, dayRange, 'A', baseOutputPath, checkExistingFiles)
    depFlightList = getFlightList(baseDate, dayRange, 'D', baseOutputPath, checkExistingFiles)

    Statistics = getStatistics(arrFlightList,depFlightList,dayRange,airlineMin)

    ProbDists = getProbabilityDistributions(arrFlightList,depFlightList,baseInputPath,minBucket,computeProbDists)

    FlightSchedule = getFlightSchedule(baseInputPath,baseDate,arrFlightList,depFlightList)

    status = writeToCSV(FlightSchedule,ProbDists,Statistics,arrFlightList,depFlightList,baseDate,dayRange,baseOutputPath)