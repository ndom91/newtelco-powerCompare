#!/usr/bin/env python3

import sys
import subprocess
import getopt
import datetime
import openpyxl
import MySQLdb
import psycopg2
import pandas as pd
import numpy as np
import argparse
import array
import math
from fuzzywuzzy import fuzz
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Border, Font, Side, Alignment, NamedStyle
from openpyxl import Workbook

def truncate(number, digits) -> float:
    stepper = pow(10.0, digits)
    return math.trunc(stepper * number) / stepper

def match_name(name, list_names, min_score=0):
    # -1 score incase we don't get any matches
    max_score = -1
    # Returning empty name for no match as well
    max_name = ""
    # Iternating over all names in the other
    for name2 in list_names:
        #Finding fuzzy match score
        score = fuzz.ratio(name, name2)
        # Checking if we are above our threshold and have a better score
        if (score > min_score) & (score > max_score):
            max_name = name2
            max_score = score
    return (max_name, max_score)

def compare(date, mailBool):
    # PSQL Connection (localhost)
    try:
        connect_str = "dbname='netbox' user='netbox' host='localhost' password='N3wt3lco'"
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        
        cursor.execute("""SELECT dcim_rack.name, dcim_rack.u_height FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1';""")
        psqlRows = cursor.fetchall()

        # # 8 - Counter Number (A-Feed)
        # cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '8';""")
        # fieldidAFeed = cursor.fetchall()
        # # print(fieldidAFeed)

        # # 11 - Counter Number (B-Feed)
        # cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '11';""")
        # fieldidBFeed = cursor.fetchall()
        # # print(fieldidBFeed)

        # 9 - Contract Power DC
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '9';""")
        fieldidDC = cursor.fetchall()
        # print(fieldidDC)

        # 9 - Contract Power AC
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '2';""")
        fieldidAC = cursor.fetchall()
        # print(fieldidAC)

        # 10 - Contract Number
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value as contract FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '10';""")
        fieldidContract = cursor.fetchall()
        # print(fieldidAC)
        cursor.close()
        conn.close()

    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)

    # MySQL Connection (94.249.164.180)

    try:
        connection = MySQLdb.connect (host = "94.249.164.180",
                                    user = "ndomino",
                                    passwd = "Miney91*",
                                    db = "newtelco_prod")

        counterValues = connection.cursor()
        q = "select powerCounters.serialNo, CONCAT(powerCounters.rNumber, ' ',powerCounters.description) as name, powerCounters.unit, powerCounterValues.dateTime, powerCounterValues.sortDateTime, powerCounterValues.value, powerCounterValues.diff FROM powerCounters left join powerCounterValues on powerCounters.id = powerCounterValues.counterId WHERE powerCounterValues.sortDateTime = %(date)s;"
        params = {'date':date}
        counterValues.execute (q, params)
        mysqlRows = counterValues.fetchall()

        yearsArray = ["201806", "201807", "201808", "201809", "201810", "201811", "201812", "201901", "201902", "201903", "201904"]
        dateVal = yearsArray.index(date)
        # print(dateVal)
        dateVal -= 1
        yearValm1 = yearsArray[dateVal]
        # print(yearValm1)
        q = "select powerCounters.serialNo, powerCounterValues.sortDateTime, powerCounterValues.value, powerCounterValues.diff FROM powerCounters left join powerCounterValues on powerCounters.id = powerCounterValues.counterId WHERE powerCounterValues.sortDateTime = %(date)s;"
        params = {'date':yearValm1}
        counterValues.execute (q, params)
        mysqlRowsm1 = counterValues.fetchall()

        dateVal -= 1
        yearValm2 = yearsArray[dateVal]
        q = "select powerCounters.serialNo, powerCounterValues.sortDateTime, powerCounterValues.value, powerCounterValues.diff FROM powerCounters left join powerCounterValues on powerCounters.id = powerCounterValues.counterId WHERE powerCounterValues.sortDateTime = %(date)s;"
        params = {'date':yearValm2}
        counterValues.execute (q, params)
        mysqlRowsm2 = counterValues.fetchall()

        counterValues.close()
        connection.close()

    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)

    # bash commands
    #subprocess.run(["ls", "-lah"])

    mysqlArr = np.asarray(mysqlRows)
    psqlArr = np.asarray(psqlRows)
    # fieldidAFeedArr = np.asarray(fieldidAFeed)
    # fieldidBFeedArr = np.asarray(fieldidBFeed)
    fieldidACArr = np.asarray(fieldidAC)
    fieldidDCArr = np.asarray(fieldidDC)
    fieldidContractArr = np.asarray(fieldidContract)
    mysqlm1Arr = np.asarray(mysqlRowsm1)
    mysqlm2Arr = np.asarray(mysqlRowsm2)

    mysqlDF = pd.DataFrame({'Counter':mysqlArr[:,0], 'name':mysqlArr[:,1], 'Month':mysqlArr[:,4], 'Counter Value':mysqlArr[:,5], 'Usage':mysqlArr[:,6]},)
    mysqlDF['Usage'] = mysqlDF['Usage'].infer_objects()

    mysqlm1DF = pd.DataFrame({'Counter':mysqlm1Arr[:,0], 'Month':mysqlm1Arr[:,1], 'Counter Value M-1':mysqlm1Arr[:,2], 'Usage M-1':mysqlm1Arr[:,3]})
    mysqlm1DF['Usage M-1'] = mysqlm1DF['Usage M-1'].infer_objects()

    mysqlm2DF = pd.DataFrame({'Counter':mysqlm2Arr[:,0], 'Month':mysqlm2Arr[:,1], 'Counter Value M-2':mysqlm2Arr[:,2], 'Usage M-2':mysqlm2Arr[:,3]})
    mysqlm2DF['Usage M-2'] = mysqlm2DF['Usage M-2'].infer_objects()


    for index, row in mysqlDF.iterrows():
        row['name'] = row['name'].replace(r' A-Feed', '')
        row['name'] = row['name'].replace(r' B-Feed', '')

    # print(mysqlDF)

    # fidAFeedDF = pd.DataFrame({'name':fieldidAFeedArr[:,0], 'value':fieldidAFeedArr[:,1]})
    # fidBFeedDF = pd.DataFrame({'name':fieldidBFeedArr[:,0], 'value':fieldidBFeedArr[:,1]})
    fidACDF = pd.DataFrame({'name':fieldidACArr[:,0], 'AC':fieldidACArr[:,1]})
    fidDCDF = pd.DataFrame({'name':fieldidDCArr[:,0], 'DC':fieldidDCArr[:,1]})
    fidContractDF = pd.DataFrame({'name':fieldidContractArr[:,0], 'Contract':fieldidContractArr[:,1]})
    psqlDF = pd.DataFrame({'name':psqlArr[:,0], 'Rack Units':psqlArr[:,1]})

    # Fuzzy Match
    # https://medium.com/@rtjeannier/combining-data-sets-with-fuzzy-matching-17efcb510ab2
    # dict_listTest = []
    # dict_list = []

    # for name in psqlDF.name:
    #     match = match_name(name, mysqlDF.name, 70)

    #     # New dict for storing data
    #     dictTest_ = {}
    #     dictTest_.update({"player_name" : name})
    #     dictTest_.update({"match_name" : match[0]})
    #     dictTest_.update({"score" : match[1]})
    #     dict_listTest.append(dictTest_)

    #     dict_ = {}
    #     dict_.update({"name" : match[0]})
    #     for i in dict_:
    #         dict_list.append(dict_[i])

    # merge_table = pd.DataFrame(dict_list)

    # # print(merge_table)
    # # print(mysqlDF)
    # n = psqlDF.columns[1]
    # psqlDF.drop(n, axis = 1, inplace = True)
    # psqlDF[n] = dict_list
    # print(psqlDF)

    merge1 = pd.merge(mysqlDF, psqlDF, left_on='name', right_on='name', how='left')
    # merge2 = pd.merge(merge1, fidAFeedDF, left_on='name', right_on='name', how='left')
    # merge3 = pd.merge(merge2, fidBFeedDF, left_on='name', right_on='name', how='left')
    merge4 = pd.merge(merge1, fidACDF, left_on='name', right_on='name', how='left')
    merge5 = pd.merge(merge4, fidDCDF, left_on='name', right_on='name', how='left')
    merge6 = pd.merge(merge5, fidContractDF, left_on='name', right_on='name', how='left')

    # print(merge6)

    merge7 = merge6.drop_duplicates().sort_values(by='name')
        
    merge8 = merge7.groupby(['Contract'])
    
    monthsArray = [31, 28, 31, 30, 31, 30, 31, 30, 31, 30, 31, 30]

    def sendMail():
        print('to: ndomino@newtelco.de')
        # print('cc: billing@newtelco.de')
        # print('cc: order@newtelco.de')
        print('From: device@newtelco.de')
        print('MIME-Version: 1.0')
        print('Content-Type: multipart/mixed; boundary=multipart-boundary')
        print('Subject: Power Comparison Prototype1 (' + date + ')')
        print('--multipart-boundary')
        print('Content-Type: text/html; charset=utf-8')
        print('')
        print('<html>')
        print('<pre>')
        print('Dear Billing,')
        print('')
        print('Here is the power output comparison for ' + date)
        print('')
        print('This includes an Excel Attachment with more thorough data!')
        print('')
        for name,group in merge8:
            print('Contract: ' + name + '<br>')
            print(group.to_string())
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(group.string())
            diffSum = group['Usage'].agg(np.sum)
            monthValue = int(date[-2:])
            # monthValue = (monthValue)
            monthValue -= 1
            monthHrs = monthsArray[int(monthValue)]
            # print(monthHrs)
            monthHrs = monthHrs * 24
            diffSum = (diffSum / monthHrs) * 1000
            diffSum = truncate(diffSum, 2)
            print('<br>')
            print('Monthly Usage (W): ' + str(diffSum) + '<br>')
            groupAC = group['AC'].max()
            print('')
            # print(groupAC)
            if str(group['AC'].max()) != 'nan':
                avgAC = group['AC'].max()
                print('Allowed Usage AC (W): ' + str(avgAC))
                diffAC = int(avgAC) - int(diffSum)
                if diffAC < 0:
                    print('<font style="color:red;font-weight:700">Difference: ' + str(diffAC) + '</font><br>')
                else: 
                    print('Difference: ' + str(diffAC) + ' Watt')
            print('')
            if str(group['DC'].max()) != 'nan':
                avgDC = group['DC'].max()
                print('Allowed Usage DC: ' + str(avgDC))
                print('Difference: ' + 'Watt')
            print('---------------------' + '<br>')

        print('</pre>')
        print('</html>')
        print('--multipart-boundary')
    
    def worksheet():
        wb = Workbook()
        ws = wb.active

        # print(mysqlm1DF)
        merge9 = pd.merge(merge6, mysqlm1DF, left_on='Counter', right_on='Counter', how='left')
        merge10 = pd.merge(merge9, mysqlm2DF, left_on='Counter', right_on='Counter', how='left')
        merge10 = merge10.drop_duplicates().sort_values(by='Contract')
        # merge10['contractDiff'] = merge10['Contract'].diff()
        merge10['contractDiff'] = merge10['Contract'] == merge10['Contract'].shift(1).fillna(merge10['Contract'])

        merge10 = merge10[pd.notnull(merge10["Contract"])]

        # print(merge10)


        monthValue = int(date[-2:])
        monthValue -= 1
        monthHrs = monthsArray[int(monthValue)]
        monthHrs = monthHrs * 24
        
        merge10.insert(5, 'Overage Month 0', '')
        merge10.insert(13, 'Overage Month 1', '')
        merge10.insert(17, 'Overage Month 2', '')
        for item, row in merge10.T.iteritems():
            # print(row.values[4])
            merge10.set_value(item,'Overage Month 0', float(row.values[4]) / float(monthHrs) * 1000.0, 3)
            # merge10.loc[row].at['Overage Month 0'] = (float(row.values[4]) / float(monthHrs) * 1000.0)
            # merge10.iat[row, 6] = float(row.values[4]) / float(monthHrs) * 1000.0
            merge10.set_value(item,'Overage Month 1', float(row.values[12]) / float(monthHrs) * 1000.0)
            merge10.set_value(item,'Overage Month 2', float(row.values[16]) / float(monthHrs) * 1000.0)
            # if row == 'False':
            #     merge10.iloc[row] = ['testt']
            # df1 = df1.assign(e=p.Series(np.random.randn(sLength)).values)

        merge10.reset_index(inplace=True)
        # print(merge10)

        contractDiffArr = np.where(merge10['contractDiff'] == False)

        i = 0
        while i < len(contractDiffArr[0]):
           b = contractDiffArr[0][i] - 0.5
           merge10.loc[b] = "test"
           i += 1

        merge10.sort_index(axis=0,inplace=True)   
        print(merge10.to_string())

        # GROUP BY CONTRACT   
        # merge11 = merge10.groupby(['Contract'])
        # for name,group in merge11:
        #     print('Contract: ' + name + '<br>')
        #     print(group.to_string())


        # Begin Excel Worksheet Manipulation

        excelRows = dataframe_to_rows(merge10)
        
        for r_idx, row in enumerate(excelRows, 1): 
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)

        for cell in ws['A'] + ws[1]:
            cell.style = 'Pandas'

        # Excel Header Names
        ws['B1'] = 'Counter Number'
        ws['C1'] = 'Rack'
        ws['D1'] = 'Month'
        ws['E1'] = 'Counter Value'
        ws['F1'] = 'Usage (Wh)'
        ws['G1'] = 'Usage (W)'
        ws['H1'] = 'Height (Units)'
        ws['I1'] = 'AC (Watts)'
        ws['J1'] = 'DC'
        ws['K1'] = 'Contract'
        ws['L1'] = 'Month'
        ws['M1'] = 'Counter Value'
        ws['N1'] = 'Usage (Wh)'
        ws['O1'] = 'Usage (W)'
        ws['P1'] = 'Month'
        ws['Q1'] = 'Counter Value'
        ws['R1'] = 'Usage (Wh)'
        ws['S1'] = 'Usage (W)'

        # Excel Formatting
        bottomBorder = NamedStyle(name="bottomBorder")
        bottomBorder.font = Font(bold=True)
        bottomBorder.alignment = Alignment(horizontal="center")
        bd = Side(style='thin', color="000000")
        bottomBorder.border = Border(bottom=bd)

        ws['A1'].style = bottomBorder
        ws['B1'].style = bottomBorder
        ws['C1'].style = bottomBorder
        ws['D1'].style = bottomBorder
        ws['E1'].style = bottomBorder
        ws['F1'].style = bottomBorder
        ws['G1'].style = bottomBorder
        ws['H1'].style = bottomBorder
        ws['I1'].style = bottomBorder
        ws['J1'].style = bottomBorder
        ws['K1'].style = bottomBorder
        ws['L1'].style = bottomBorder
        ws['M1'].style = bottomBorder
        ws['N1'].style = bottomBorder
        ws['O1'].style = bottomBorder
        ws['P1'].style = bottomBorder

        # ID
        ws.column_dimensions['A'].width = 7
        # counter number
        ws.column_dimensions['B'].width = 17
        # rack description
        ws.column_dimensions['C'].width = 30
        # month (date)
        ws.column_dimensions['D'].width = 10
        # power usage
        ws.column_dimensions['E'].width = 15
        # diff
        ws.column_dimensions['F'].width = 10
        # height (rack units)
        ws.column_dimensions['G'].width = 15
        # ac (watts)
        ws.column_dimensions['H'].width = 12
        # dc
        ws.column_dimensions['I'].width = 12
        # Contract
        ws.column_dimensions['J'].width = 12
        # Month
        ws.column_dimensions['K'].width = 10
        # Counter Value
        ws.column_dimensions['L'].width = 15
        # Usage
        ws.column_dimensions['M'].width = 15
        # Month
        ws.column_dimensions['N'].width = 15
        # Counter Value
        ws.column_dimensions['O'].width = 15
        # Usage
        ws.column_dimensions['P'].width = 15
        # Usage
        ws.column_dimensions['Q'].width = 15
        # Month
        ws.column_dimensions['R'].width = 15
        # Counter Value
        ws.column_dimensions['S'].width = 15
        # Usage
        ws.column_dimensions['T'].width = 15

        ws.column_dimensions.group('A', hidden=True)
        ws.insert_cols(2)
        for cell in ws['H']:
            cell.style = 'Comma'
        for cell in ws['P']:
            cell.style = 'Comma'
        for cell in ws['T']:
            cell.style = 'Comma'
        
        ws['H1'].style = bottomBorder
        ws['P1'].style = bottomBorder
        ws['T1'].style = bottomBorder
        filenameDate = datetime.datetime.now().strftime("%d%m%Y")
        # wb.save("output/excel/powerCompare_" + date + "_" + filenameDate + ".xlsx")


    if mailBool == 0:
        worksheet()
        sendMail()
    else:
        worksheet()

def main(argv):
    selectedDate = ''
    sendMail = 1

    try:
      opts, args = getopt.getopt(argv,"hd:m",["help", "date=", "sendmail"])
    except getopt.GetoptError:
      print('Usage: power.py -d <date i.e. 201808>')
      sys.exit(2)
    for opt, arg in opts:
      if opt in ('-h', '--help'):
        print('Usage: power.py -d <date i.e. 201808>')
        sys.exit()
      elif opt in ("-d", "--date"):
        selectedDate = arg
      elif opt in ("-m", "--sendmail"):
        sendMail = 0

         
    compare(selectedDate, sendMail)

if __name__ == "__main__":
    main(sys.argv[1:])

