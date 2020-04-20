import sys, getopt

def prepare_2016_csv(infilename,outfilename, parkname,code):
    with open(infilename,'r') as infile:
        with open(outfilename, 'a') as outfile:
            for line in infile:
                #print(line.find(" 2016 ") != -1,(line.find(" Jun ") != -1 or line.find(" Jul ") != -1 or line.find(" Aug ") != -1 or line.find(" Sep ") != -1 or line.find(" Oct ") != -1))
                if line.find(" 2016 ") != -1 and (line.find(" Jun ") != -1 or line.find(" Jul ") != -1 or line.find(" Aug ") != -1 or line.find(" Sep ") != -1 or line.find(" Oct ") != -1):
                    #line = line.rstrip('\n')
                    line = line.replace(parkname, str(code) +",")
                    line = line.replace(" free parking @","")
                    line = line.replace("Mon ",",d1,")
                    line = line.replace("Tue ",",d2,")
                    line = line.replace("Wed ",",d3,")
                    line = line.replace("Thu ",",d4,")
                    line = line.replace("Fri ",",d5,")
                    line = line.replace("Sat ",",d6,")
                    line = line.replace("Sun ",",d7,")
                    line = line.replace("Jan ", "01,")
                    line = line.replace("Feb ", "02,")
                    line = line.replace("Mar ", "03,")
                    line = line.replace("Apr ", "04,")
                    line = line.replace("May ", "05,")
                    line = line.replace("Jun ", "06,")
                    line = line.replace("Jul ", "07,")
                    line = line.replace("Aug ", "08,")
                    line = line.replace("Sep ", "09,")
                    line = line.replace("Oct ", "10,")
                    line = line.replace("Nov ", "11,")
                    line = line.replace("Dec ", "12,")
                    line = line.replace(" 2016 ", ",2016,")
                    line = line.replace("GMT+0100 (CET)","")
                    line = line.replace("GMT+0200 (CEST)","")
                    outfile.write(line)

'''try:
    opts, args = getopt.getopt(sys.argv[1:], "", [])
except getopt.GetoptError:
    print("wrong arguments")
    sys.exit(2)'''
for _ in range(0,7):
    if _ == 0:
        path = r"C:\Users\Utente\Desktop\Temp APPUNTONI IN WORD\Temp Web Science\parcheggi\log_Parcheggio Centro Lago.txt"
        parkname = "Parking 103(Parcheggio Centro Lago): "
        title = r"Pred103.txt"
        code = 103
    if _ == 1:
        path = r"C:\Users\Utente\Desktop\Temp APPUNTONI IN WORD\Temp Web Science\parcheggi\log_Autosilo Auguadri.txt"
        parkname = "Parking 101(Autosilo Auguadri): "
        title = r"Pred101.txt"
        code = 101
    if _ == 2:
        path = r"C:\Users\Utente\Desktop\Temp APPUNTONI IN WORD\Temp Web Science\parcheggi\log_Autosilo Valduce.txt"
        parkname = "Parking 104(Autosilo Valduce): "
        title = r"Pred104.txt"
        code = 104
    if _ == 3:
        path = r"C:\Users\Utente\Desktop\Temp APPUNTONI IN WORD\Temp Web Science\parcheggi\log_Autosilo Valmulini.txt"
        parkname = "Parking 123(Autosilo Valmulini): "
        title = r"Pred123.txt"
        code = 123
    if _ == 4:
        path = r"C:\Users\Utente\Desktop\Temp APPUNTONI IN WORD\Temp Web Science\parcheggi\log_Parcheggio Castelnuovo.txt"
        parkname = "Parking 107(Parcheggio Castelnuovo): "
        title = r"Pred107.txt"
        code = 107
    if _ == 5:
        path = r"C:\Users\Utente\Desktop\Temp APPUNTONI IN WORD\Temp Web Science\parcheggi\log_Parcheggio Como San Giovanni.txt"
        parkname = "Parking 109(Parcheggio Como San Giovanni): "
        title = r"Pred109.txt"
        code = 109
    if _ == 6:
        path = r"C:\Users\Utente\Desktop\Temp APPUNTONI IN WORD\Temp Web Science\parcheggi\log_Parcheggio Sirtori.txt"
        parkname = "Parking 105(Parcheggio Sirtori): "
        title = r"Pred105.txt"
        code = 105

    title = r"AllFiltParkPred.txt" #comment out this line to have multiple files

    prepare_2016_csv(path,title,parkname,code)

print("Finished")