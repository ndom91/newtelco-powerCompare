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
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Border, Font, Side, Alignment, NamedStyle
from openpyxl import Workbook

def compare(date, boolExcel):
    # PSQL Connection (localhost)
    try:
        connect_str = "dbname='netbox' user='netbox' host='localhost' " + "password='N3wt3lco'"
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        
        cursor.execute("""SELECT dcim_rack.name, dcim_rack.u_height FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1';""")
        psqlRows = cursor.fetchall()

        # 8 - Counter Number (A-Feed)
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '8';""")
        fieldidAFeed = cursor.fetchall()
        # print(fieldidAFeed)

        # 11 - Counter Number (B-Feed)
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '11';""")
        fieldidBFeed = cursor.fetchall()
        # print(fieldidBFeed)

        # 9 - Contract Power DC
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '9';""")
        fieldidDC = cursor.fetchall()
        # print(fieldidDC)

        # 9 - Contract Power AC
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '2';""")
        fieldidAC = cursor.fetchall()
        # print(fieldidAC)

        # 10 - Contract Number
        cursor.execute("""SELECT dcim_rack.name, extras_customfieldvalue.serialized_value FROM dcim_rack LEFT JOIN extras_customfieldvalue ON dcim_rack.id = extras_customfieldvalue.obj_id WHERE dcim_rack.site_id = '1' AND extras_customfieldvalue.field_id = '10';""")
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
        # serialNo  |  description  |  RackNumber  |  unit(s)  |  dateTime  |  sortDateTime  |  counter value  |  counter diff

        mysqlRows = counterValues.fetchall()
        counterValues.close()
        connection.close()

    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)

    # bash commands
    #subprocess.run(["ls", "-lah"])

    mysqlArr = np.asarray(mysqlRows)
    psqlArr = np.asarray(psqlRows)
    fieldidAFeedArr = np.asarray(fieldidAFeed)
    fieldidBFeedArr = np.asarray(fieldidBFeed)
    fieldidACArr = np.asarray(fieldidAC)
    fieldidDCArr = np.asarray(fieldidDC)
    fieldidContractArr = np.asarray(fieldidContract)

    mysqlDF = pd.DataFrame({'serialNo':mysqlArr[:,0], 'name':mysqlArr[:,1], 'sortDateTime':mysqlArr[:,4], 'value':mysqlArr[:,5], 'diff':mysqlArr[:,6]})

    for index, row in mysqlDF.iterrows():
        row['name'] = row['name'].replace(r'A-Feed', '')
        row['name'] = row['name'].replace(r'B-Feed', '')

    # print(mysqlDF)

    fidAFeedDF = pd.DataFrame({'name':fieldidAFeedArr[:,0], 'value':fieldidAFeedArr[:,1]})
    fidBFeedDF = pd.DataFrame({'name':fieldidBFeedArr[:,0], 'value':fieldidBFeedArr[:,1]})
    fidACDF = pd.DataFrame({'name':fieldidACArr[:,0], 'value':fieldidACArr[:,1]})
    fidDCDF = pd.DataFrame({'name':fieldidDCArr[:,0], 'value':fieldidDCArr[:,1]})
    fidContractDF = pd.DataFrame({'name':fieldidContractArr[:,0], 'value':fieldidContractArr[:,1]})
    psqlDF = pd.DataFrame({'name':psqlArr[:,0], 'height':psqlArr[:,1]})


    merge1 = pd.merge(mysqlDF, psqlDF, left_on='name', right_on='name', how='left')
    merge2 = pd.merge(merge1, fidAFeedDF, left_on='name', right_on='name', how='left')
    merge3 = pd.merge(merge2, fidBFeedDF, left_on='name', right_on='name', how='left')
    merge4 = pd.merge(merge3, fidACDF, left_on='name', right_on='name', how='left')
    merge5 = pd.merge(merge4, fidDCDF, left_on='name', right_on='name', how='left')
    merge6 = pd.merge(merge5, fidContractDF, left_on='name', right_on='name', how='left')

    merge7 = merge6.drop_duplicates().sort_values(by='name')
    # print(merge7)
    # merge7.groupby(['']).sum()

    # Test Print!
    # print(merge7)
    if boolExcel == '0':
        print(merge7)
    else:
        wb = Workbook()
        ws = wb.active

        excelRows = dataframe_to_rows(merge7)
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
        ws['F1'] = 'Diff (vs. Last Month)'
        ws['G1'] = 'Height (Units)'
        ws['H1'] = ''
        ws['I1'] = ''
        ws['J1'] = 'AC (Watts)'
        ws['K1'] = 'DC '
        ws['L1'] = 'Contract'

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
        ws.column_dimensions['F'].width = 20
        # height (rack units)
        ws.column_dimensions['G'].width = 15
        # a-feed counter *HIDDEN*
        ws.column_dimensions['H'].width = 12
        # b-feed counter *HIDDEN*
        ws.column_dimensions['I'].width = 12
        # ac (watts)
        ws.column_dimensions['J'].width = 12
        # dc
        ws.column_dimensions['K'].width = 7
        # contract
        ws.column_dimensions['L'].width = 10

        ws.column_dimensions.group('H','I', hidden=True)
        filenameDate = datetime.datetime.now().strftime("%d%m%Y")
        wb.save("powerCompare_" + date + "_" + filenameDate + ".xlsx")

def main(argv):
    selectedDate = ''
    noExcel = 1

    try:
      opts, args = getopt.getopt(argv,"hd:ne",["help", "date=", "noexcel"])
    except getopt.GetoptError:
      print('Usage: power.py -d <date i.e. 201808>')
      sys.exit(2)
    for opt, arg in opts:
      if opt in ('-h', '--help'):
        print('Usage: power.py -d <date i.e. 201808>')
        sys.exit()
      elif opt in ("-d", "--date"):
        selectedDate = arg
      elif opt in ("-ne", "-noexcel"):
        noExcel = '0'

         
    compare(selectedDate, noExcel)

if __name__ == "__main__":
    main(sys.argv[1:])

