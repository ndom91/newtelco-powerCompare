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
import dbconfig as cfg
from fuzzywuzzy import fuzz
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Border, Font, Side, Alignment, Fill, PatternFill, NamedStyle
from openpyxl.worksheet import worksheet
from openpyxl import Workbook

def truncate(number, digits) -> float:
    stepper = pow(10.0, digits)
    return math.trunc(stepper * number) / stepper

def move_cell(source_cell, coord, tgt):
    tgt[coord].value = source_cell.value
    if source_cell.has_style:
        tgt[coord]._style = copy(source_cell._style)

    del source_cell.parent._cells[(source_cell.row, source_cell.col_idx)]

    return tgt[coord]

def compare(date, mailBool):
    # PSQL Connection (localhost)
    try:
        
        psql_db = cfg.psql['database']
        psql_user = cfg.psql['user']
        psql_host = cfg.psql['host']
        psql_pw = cfg.psql['password']

        conn = psycopg2.connect(dbname=psql_db, user=psql_user, host=psql_host, password=psql_pw)

        cursor = conn.cursor()
        
        cursor.execute("""SELECT dcim_rack.name FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1';""")
        psqlRows = cursor.fetchall()

        # 9 - Contract Power DC
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '9';""")
        fieldidDC = cursor.fetchall()

        # 9 - Contract Power AC
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '2';""")
        fieldidAC = cursor.fetchall()

        # 10 - Contract Number
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value as contract FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '10';""")
        fieldidContract = cursor.fetchall()

        cursor.close()
        conn.close()

    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)

    try:

        connection = MySQLdb.connect(cfg.mysql['host'],cfg.mysql['user'],cfg.mysql['passwd'],cfg.mysql['db'])

        counterValues = connection.cursor()
        q = "select powerCounters.serialNo, CONCAT(powerCounters.rNumber, ' ',powerCounters.description) as name, powerCounters.rNumber, powerCounterValues.sortDateTime, powerCounterValues.diff FROM powerCounters left join powerCounterValues on powerCounters.id = powerCounterValues.counterId WHERE powerCounterValues.sortDateTime = %(date)s;"
        params = {'date':date}
        counterValues.execute (q, params)
        mysqlRows = counterValues.fetchall()

        yearsArray = ["201806", "201807", "201808", "201809", "201810", "201811", "201812", "201901", "201902", "201903", "201904"]
        dateVal = yearsArray.index(date)
        # print(dateVal)
        dateVal -= 1
        yearValm1 = yearsArray[dateVal]
        # print(yearValm1)
        q = "select powerCounters.serialNo, powerCounterValues.sortDateTime, powerCounterValues.diff FROM powerCounters left join powerCounterValues on powerCounters.id = powerCounterValues.counterId WHERE powerCounterValues.sortDateTime = %(date)s;"
        params = {'date':yearValm1}
        counterValues.execute (q, params)
        mysqlRowsm1 = counterValues.fetchall()

        dateVal -= 1
        yearValm2 = yearsArray[dateVal]
        q = "select powerCounters.serialNo, powerCounterValues.sortDateTime, powerCounterValues.diff FROM powerCounters left join powerCounterValues on powerCounters.id = powerCounterValues.counterId WHERE powerCounterValues.sortDateTime = %(date)s;"
        params = {'date':yearValm2}
        counterValues.execute (q, params)
        mysqlRowsm2 = counterValues.fetchall()

        counterValues.close()
        connection.close()

    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)

    mysqlArr = np.asarray(mysqlRows)
    psqlArr = np.asarray(psqlRows)
    fieldidACArr = np.asarray(fieldidAC)
    fieldidDCArr = np.asarray(fieldidDC)
    fieldidContractArr = np.asarray(fieldidContract)
    mysqlm1Arr = np.asarray(mysqlRowsm1)
    mysqlm2Arr = np.asarray(mysqlRowsm2)

    mysqlDF = pd.DataFrame({'Counter':mysqlArr[:,0], 'name':mysqlArr[:,1], 'Rack':mysqlArr[:,2], 'Month':mysqlArr[:,3], 'Usage':mysqlArr[:,4]},)
    mysqlDF['Usage'] = mysqlDF['Usage'].infer_objects()

    mysqlm1DF = pd.DataFrame({'Counter':mysqlm1Arr[:,0], 'Month-1':mysqlm1Arr[:,1], 'Usage M-1':mysqlm1Arr[:,2]})
    mysqlm1DF['Usage M-1'] = mysqlm1DF['Usage M-1'].infer_objects()

    mysqlm2DF = pd.DataFrame({'Counter':mysqlm2Arr[:,0], 'Month-2':mysqlm2Arr[:,1], 'Usage M-2':mysqlm2Arr[:,2]})
    mysqlm2DF['Usage M-2'] = mysqlm2DF['Usage M-2'].infer_objects()


    for index, row in mysqlDF.iterrows():
        row['name'] = row['name'].replace(r' A-Feed', '')
        row['name'] = row['name'].replace(r' B-Feed', '')

    fidACDF = pd.DataFrame({'name':fieldidACArr[:,0], 'AC':fieldidACArr[:,1]})
    fidDCDF = pd.DataFrame({'name':fieldidDCArr[:,0], 'DC':fieldidDCArr[:,1]})
    fidContractDF = pd.DataFrame({'name':fieldidContractArr[:,0], 'Contract':fieldidContractArr[:,1]})
    psqlDF = pd.DataFrame({'name':psqlArr[:,0]})

    merge1 = pd.merge(mysqlDF, psqlDF, left_on='name', right_on='name', how='left')
    merge4 = pd.merge(merge1, fidACDF, left_on='name', right_on='name', how='left')
    merge5 = pd.merge(merge4, fidDCDF, left_on='name', right_on='name', how='left')
    merge6 = pd.merge(merge5, fidContractDF, left_on='name', right_on='name', how='left')

    merge7 = merge6.drop_duplicates().sort_values(by='name')
        
    
    monthsArray = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    #               J   F   M   A   M   J   J   A   S   O   N   D

    def sendMail():
        print('to: service@newtelco.de')
        print('cc: power@newtelco.de')
        print('cc: billing@newtelco.de')
        print('cc: order@newtelco.de')
        print('cc: sales@newtelco.de')
        print('From: device@newtelco.de')
        print('MIME-Version: 1.0')
        print('Content-Type: multipart/mixed; boundary=multipart-boundary')
        print('Subject: [POWER USAGE] Monthly Power Comparison (' + date + ')')
        print('--multipart-boundary')
        print('Content-Type: text/html; charset=utf-8')
        print('')
        print('<html>')
        print('<pre>')
        print('Dear Colleagues,')
        print('')
        print('Below is the power usage comparison for ' + date)
        print('')
        print('Please see the Excel Attachment for more thorough data')
        print('Note: this is an beta version, if you find any errors - please report to ndomino@newtelco.de')
        print('')

        rackAC0 = merge7.filter(['Rack','Contract','AC'], axis=1).drop_duplicates()
        rackAC0['AC'] = pd.to_numeric(rackAC0['AC'])
        rackAC0 = rackAC0.sort_values(by='Contract')
        rackAC0 = rackAC0.groupby(['Contract'])['AC'].sum()
        # print(rackAC0)
        merge81 = pd.merge(merge7, rackAC0, left_on='Contract', right_on='Contract', how='left')

        merge81 = merge81.sort_values(by='Contract')

        merge81 = merge81.drop(['AC_x'], axis=1)
        merge82 = merge81.groupby(['Contract'])

        for name,group in merge82:
            diffSum = pd.to_numeric(group['Usage']).sum()
            monthValue = int(date[-2:])
            monthValue -= 1
            monthHrs = monthsArray[int(monthValue)]
            monthHrs = int(monthHrs * 24)
            diffSum = (diffSum / monthHrs) * 1000
            diffSum = truncate(diffSum, 2)
            groupAC = group['AC_y'].max()
            if str(group['AC_y'].max()) != 'nan' and str(group['AC_y'].max()) != '0.0':
                avgAC = group['AC_y'].max()
                diffAC = int(avgAC) - int(diffSum)
                if diffAC < 0:
                    print('Contract: <font style="weight:700">' + name + '</font><br>')
                    print(group.to_string())
                    print('')
                    diffSum = (diffSum / 1000) 
                    avgAC = (avgAC / 1000) 
                    diffSum = float("{0:.2f}".format(diffSum))
                    print('Monthly Usage: ' + str(diffSum) + ' kW')
                    print('Allowed Usage: ' + str(avgAC) + ' kW')
                    diffAC = (diffAC / 1000) * -1
                    diffAC = float("{0:.2f}".format(diffAC))
                    print('<font style="color:red;font-weight:700">Over Usage (Überverbrauch): ' + str(diffAC) + ' kW</font><br>')
                    if str(group['DC'].max()) != 'nan':
                        avgDC = group['DC'].max()
                        print('Allowed Usage DC: ' + str(avgDC))
                        print('Over Usage (Überverbrauch): ' + ' Watt ')
                    print('---------------------' + '<br>')
                # else: 
                #     print('Difference: ' + str(diffAC) + ' Watt')
            

        print('</pre>')
        print('</html>')
        print('--multipart-boundary')
    
    def worksheet():
        wb = Workbook()
        ws = wb.active

        # print(mysqlm1DF)
        merge9 = pd.merge(merge6, mysqlm1DF, left_on='Counter', right_on='Counter', how='left')
        merge10 = pd.merge(merge9, mysqlm2DF, left_on='Counter', right_on='Counter', how='left')
        merge10 = merge10.drop_duplicates().sort_values(by=['Contract','name'])
        merge10['contractDiff'] = merge10['Contract'] == merge10['Contract'].shift(1).fillna(merge10['Contract'])

        merge10 = merge10[pd.notnull(merge10["Contract"])]

        # print(merge10)

        monthValue = int(date[-2:])
        monthValue -= 1
        monthHrs = monthsArray[int(monthValue)]
        monthHrs = monthHrs * 24
        
        merge10.insert(5, 'Overage Month 0', '')
        merge10.insert(11, 'Overage Month 1', '')
        merge10.insert(14, 'Overage Month 2', '')
        # print(merge10)
        for item, row in merge10.T.iteritems():
            merge10.set_value(item,'Overage Month 0', float(row.values[4]) / float(monthHrs) * 1000.0, 3)
            merge10.set_value(item,'Overage Month 1', float(row.values[10]) / float(monthHrs) * 1000.0)
            merge10.set_value(item,'Overage Month 2', float(row.values[13]) / float(monthHrs) * 1000.0)

        merge10.reset_index(inplace=True)
        # print(merge10)
        
        merge10['Overage Month 0'] = pd.to_numeric(merge10['Overage Month 0'])
        merge10['Overage Month 1'] = pd.to_numeric(merge10['Overage Month 1'])
        merge10['Overage Month 2'] = pd.to_numeric(merge10['Overage Month 2'])

        merge10.insert(7,'overage_sum0','')
        merge10['overage_sum0'] = merge10.groupby(['Contract'])['Overage Month 0'].cumsum()

        merge10.insert(14,'overage_sum1','')
        merge10['overage_sum1'] = merge10.groupby(['Contract'])['Overage Month 1'].cumsum()

        merge10.insert(18,'overage_sum2','')
        merge10['overage_sum2'] = merge10.groupby(['Contract'])['Overage Month 2'].cumsum()

        
        # merge10.insert(9,'allowed_contract_sum','')
        merge10['AC'] = pd.to_numeric(merge10['AC'])

        # print(merge11)
        rackAC = merge10.filter(['Rack','Contract','AC'], axis=1).drop_duplicates()
        rackAC = rackAC.sort_values(by='Contract')
        rackAC = rackAC.groupby(['Contract'])['AC'].sum()
        merge12 = pd.merge(merge10, rackAC, left_on='Contract', right_on='Contract', how='left')
        # print(merge12)

        sumOverage = merge10.filter(['name','Contract','Overage Month 0'], axis=1)
        sumOverage = sumOverage.sort_values(by='Contract')
        sumOverage = sumOverage.groupby(['Contract'])['Overage Month 0'].sum()
        merge13 = pd.merge(merge12, sumOverage, left_on='Contract', right_on='Contract', how='left')
        # print(merge13)

        sumOverage1 = merge10.filter(['name','Contract','Overage Month 1'], axis=1)
        sumOverage1 = sumOverage1.sort_values(by='Contract')
        sumOverage1 = sumOverage1.groupby(['Contract'])['Overage Month 1'].sum()
        merge14 = pd.merge(merge13, sumOverage1, left_on='Contract', right_on='Contract', how='left')
        # print(merge13)

        sumOverage2 = merge10.filter(['name','Contract','Overage Month 2'], axis=1)
        sumOverage2 = sumOverage2.sort_values(by='Contract')
        sumOverage2 = sumOverage2.groupby(['Contract'])['Overage Month 2'].sum()
        merge15 = pd.merge(merge14, sumOverage2, left_on='Contract', right_on='Contract', how='left')
        # print(merge15)

        # Begin Excel Worksheet Manipulation

        excelRows = dataframe_to_rows(merge15)
        
        for r_idx, row in enumerate(excelRows, 1): 
            for c_idx, value in enumerate(row, 1):
                ws.cell(row=r_idx, column=c_idx, value=value)



        ws.insert_cols(2)
        ws.insert_cols(5, amount=2)
        ws.insert_cols(6, amount=2)
        ws.insert_cols(21)
        ws.insert_cols(24)
        ws.insert_cols(25)

        i = 3
        rowCount = ws.max_row
        while i <= rowCount: 
            move_cell(ws['Q' + str(i)],'F' + str(i),ws)
            move_cell(ws['I' + str(i)],'E' + str(i),ws)
            move_cell(ws['L' + str(i)],'I' + str(i),ws)
            move_cell(ws['AD' + str(i)],'G' + str(i),ws)
            move_cell(ws['AE' + str(i)],'H' + str(i),ws)
            move_cell(ws['H' + str(i)],'L' + str(i),ws)
            move_cell(ws['AF' + str(i)],'V' + str(i),ws)
            move_cell(ws['AA' + str(i)],'X' + str(i),ws)
            move_cell(ws['Z' + str(i)],'Y' + str(i),ws)
            move_cell(ws['AG' + str(i)],'Z' + str(i),ws)
            move_cell(ws['I' + str(i)],'H' + str(i),ws)
            move_cell(ws['M' + str(i)],'I' + str(i),ws)
            move_cell(ws['H' + str(i)],'M' + str(i),ws)
            move_cell(ws['K' + str(i)],'H' + str(i),ws)
            move_cell(ws['I' + str(i)],'K' + str(i),ws)
            move_cell(ws['M' + str(i)],'I' + str(i),ws)
            move_cell(ws['X' + str(i)],'AA' + str(i),ws)
            move_cell(ws['Y' + str(i)],'X' + str(i),ws)
            move_cell(ws['AA' + str(i)],'Y' + str(i),ws)
            i += 1

        # Excel Header Names
        ws['B1'] = ''
        ws['C1'] = ''
        ws['D1'] = 'Counter'
        ws['E1'] = 'Rack'
        ws['F1'] = 'Contract'
        ws['G1'] = 'AC Allowed (W)'
        ws['H1'] = 'Month'
        ws['I1'] = 'Usage (Wh)'
        ws['J1'] = 'Usage(W)'
        ws['K1'] = 'Usage(W)'
        ws['L1'] = 'Usage Sum (W)'
        ws['M1'] = 'Usage(W)'
        ws['N1'] = 'Contract'
        ws['O1'] = 'Rack Allowed'
        ws['P1'] = ''
        ws['Q1'] = 'Contract'
        ws['R1'] = 'Month-1'
        ws['S1'] = 'Usage (Wh)'
        ws['T1'] = 'Usage (W)'
        ws['U1'] = ''
        ws['V1'] = 'Usage Sum (W)'
        ws['W1'] = 'Month -2'
        ws['X1'] = 'Usage (Wh)'
        ws['Y1'] = 'Usage (W)'
        ws['Z1'] = 'Usage Sum (W)'
        ws['AA1'] = ''
        ws['AB1'] = ''

        # ID
        ws.column_dimensions['A'].width = 7
        # counter number
        ws.column_dimensions['B'].width = 13
        # rack description
        ws.column_dimensions['C'].width = 13
        # month (date)
        ws.column_dimensions['D'].width = 10
        # power usage
        ws.column_dimensions['E'].width = 27
        # diff
        ws.column_dimensions['F'].width = 10
        # height (rack units)
        ws.column_dimensions['G'].width = 17
        # ac (watts)
        ws.column_dimensions['H'].width = 18
        # dc
        ws.column_dimensions['I'].width = 15
        # Contract
        ws.column_dimensions['J'].width = 12
        # Month
        ws.column_dimensions['K'].width = 18
        # Counter Value
        ws.column_dimensions['L'].width = 17
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
        ws.column_dimensions['V'].width = 20
        ws.column_dimensions['W'].width = 15
        ws.column_dimensions['X'].width = 15
        ws.column_dimensions['Y'].width = 20
        ws.column_dimensions['Z'].width = 20

        ws.column_dimensions.group('A','C', hidden=True)
        ws.column_dimensions.group('M','Q', hidden=True)
        ws.column_dimensions.group('J', hidden=True)
        ws.column_dimensions.group('U', hidden=True)
        ws.column_dimensions.group('AA','AG', hidden=True)


        for cell in ws['K']:
            cell.style = 'Comma'
        for cell in ws['L']:
            cell.style = 'Comma'
        for cell in ws['M']:
            cell.style = 'Comma'
        for cell in ws['T']:
            cell.style = 'Comma'
        for cell in ws['V']:
            cell.style = 'Comma'
        for cell in ws['Y']:
            cell.style = 'Comma'
        for cell in ws['Z']:
            cell.style = 'Comma'
            
        key_column = 6
        merge_columns = [6, 7, 12, 22, 26]
        start_row = 3
        max_row = ws.max_row
        key = None

        # Iterate all rows in `key_colum`
        for row, row_cells in enumerate(ws.iter_rows(min_col=key_column, min_row=start_row,
                                             max_col=key_column, max_row=max_row),
                                start_row):
            if key != row_cells[0].value or row == max_row:
                # moved line below this if
                # if row == max_row: row += 1 
                if not key is None:
                    for merge_column in merge_columns:
                        ws.merge_cells( start_row=start_row, start_column=merge_column,
                                        end_row=row - 1, end_column=merge_column)

                        ws.cell(row=start_row, column=merge_column).\
                            alignment = Alignment(horizontal='center', vertical='center')

                    start_row = row

                key = row_cells[0].value
            #moved below line here as it was merging last two rows content even if the values differ.
            if row == max_row: row += 1 

        rh1 = ws.row_dimensions[1]
        rh1.height = 25

        for cell in ws['A'] + ws[1]:
            cell.style = 'Headline 2'
            cell.alignment = Alignment(horizontal="center", vertical="center")
            

         # Excel Formatting
        bottomBorder = NamedStyle(name="bottomBorder")
        # bottomBorder.font = Font(bold=True)
        bottomBorder.alignment = Alignment(horizontal="center")
        bottomBorder.alignment = Alignment(vertical="center")
        bottomBorder.alignment = Alignment(wrapText=1)
        bottomBorder.font = Font(color="ffffff", name="Calibri", size="12", bold=True)
        bottomBorder.fill = PatternFill("solid", fgColor="67B246")
        bd = Side(style='thin', color="ffffff")
        bottomBorder.border = Border(bottom=bd, right=bd, left=bd)
        
        for cell in ws['A'] + ws[1]:
            cell.style = bottomBorder
            cell.alignment = Alignment(horizontal="center", vertical="center")

        # for cell in ws
        ws.sheet_view.zoomScale = 75

        filenameDate = datetime.datetime.now().strftime("%d%m%Y")
        wb.save("/var/www/html/powercompare/output/excel/powerCompare_" + date + "_" + filenameDate + ".xlsx")


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

