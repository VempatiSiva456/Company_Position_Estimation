# including required modules
import csv
import sqlite3

# creating a database connection
op = sqlite3.connect("record.db")

# creating a cursor to the database connection
c = op.cursor()

# creating Ticker table
c.execute(
    """CREATE TABLE  Ticker(
        Date_ text,
        Company_Name text,
        Industry text,
        Previous_Day_Price float,
        Current_Price float,
        Change_In_Price float,
        Confidance text
    )"""
)

# creating two temporary tables to insert data from tickers
# to do calculations to know best and worst companies
c.execute(
    """CREATE TABLE Temp1(
        Company_Name text,
        First_Day_Price float,
        Current_Price float,
        Change_In_Price float
    )
    """
)

c.execute(
    """CREATE TABLE Temp2(
        Change float,
        Names text,
        Percent float
    )
    """
)

# creating Metrics table to insert data to this according to the conditions
c.execute(
    """CREATE TABLE Metrics(
        KPIs text,
        Metrics text
    )
    """
)

# defining a new list to store upper and lower limit values from control table
limits = []

# reading conditions given in control-table
# after converting the table into csv file
with open("Control/control-table.csv", "r") as f:
    # using csv.reader
    data = csv.reader(f)
    header = next(f)
    for line in data:
        # strip the symbols <>=% from the given data
        limits.append(line[1].strip("<>=% "))

# definining lower limit and upper limit for each industry to control

fg_low = int(limits[0])
fg_high = int(limits[2])

aa_low = int(limits[3])
aa_high = int(limits[5])

cg_low = int(limits[6])
cg_high = int(limits[8])

# defining confidance (Initially)
confidance = "Listed New"

# storing the path of each file in a list
# to read from one by one using index values of path
paths = ["Record/2021101135-20-05-2022.csv",
         "Record/2021101135-21-05-2022.csv",
         "Record/2021101135-22-05-2022.csv",
         "Record/2021101135-23-05-2022.csv",
         "Record/2021101135-24-05-2022.csv"]

# defining a new list to store dates in a list to insert using index of date
dates = []

for i in range(0, 5):
    # extracting date from the csv file name as we are saving that with name
    dates.append(paths[i][18:28])

# defining a new list to store values from 1st day i.e. on 20-05-2022
day_1 = []

# iterating in loop ranging from 0 to (5 -> (5-1))
for i in range(0, 5):
    # opening files using index values of list paths
    with open(paths[i]) as f:
        data = csv.reader(f)
        header = next(f)
        j = 0
        # making a condition that if it is day1
        # then assign NA to previous calculating values
        if i == 0:
            for line in data:
                day_1.append([dates[i], line[0], line[1],
                              "NA", line[2], "NA", confidance])
            for line in day_1:
                # inserting data obtained from csv file and all to Ticker table
                c.execute(
                    """INSERT INTO Ticker VALUES(?, ?, ?, ?, ?, ?, ?)
                    """, line)
        # now for days remaining
        else:
            rem_days = []
            for line in data:
                # calculating change in price
                change = float(line[2]) - float(day_1[j][4])
                # calculating change in price (%)
                change_perc = float((change * 100) / float(day_1[j][4]))
                change_perc = round(change_perc, 10)
                # for Industry "Finance - General"
                if line[1] == "Finance - General":
                    # checking conditions
                    if change_perc < fg_low:
                        confidance = "Low"
                    elif change_perc > fg_high:
                        confidance = "High"
                    else:
                        confidance = "Medium"
                # for Industry "Auto Ancillaries"
                elif line[1] == "Auto Ancillaries":
                    # checking conditions
                    if change_perc < aa_low:
                        confidance = "Low"
                    elif change_perc > aa_high:
                        confidance = "High"
                    else:
                        confidance = "Medium"
                # for Industry "Ceramics & Granite"
                else:
                    # checking conditions
                    if change_perc < cg_low:
                        confidance = "Low"
                    elif change_perc > cg_high:
                        confidance = "High"
                    else:
                        confidance = "Medium"
                # append this list of data in remdays list
                rem_days.append([dates[i], line[0], line[1],
                                 day_1[j][4], line[2],
                                 change_perc, confidance])
                j += 1
            # Insert values in Ticker table
            for line in rem_days:
                c.execute(
                    """INSERT INTO Ticker VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, line)
            day_1 = rem_days

# define 2 strings to store Best Listed Industry & Worst Listed Industry
best_performing_industry = ""
worst_performing_industry = ""

# calculating number of highs each industry got
# Assign 0 to variables initially
fg_nhighs = 0
aa_nhighs = 0
cg_nhighs = 0

# now, increment if Confidance is High in that particular Industry
c.execute("SELECT Industry FROM Ticker WHERE Confidance == 'High'")
high_info = c.fetchall()

for line in high_info:
    if line[0] == "Finance - General":
        fg_nhighs += 1
    elif line[0] == "Auto Ancillaries":
        aa_nhighs += 1
    else:
        cg_nhighs += 1

# Checking which Industry is Best Listed Indusrty
# Based on the Max no. of Highs it got
if(fg_nhighs >= aa_nhighs and fg_nhighs >= cg_nhighs):
    best_performing_industry = "Finance - General"
elif(aa_nhighs >= fg_nhighs and aa_nhighs >= cg_nhighs):
    best_performing_industry = "Auto Ancillaries"
else:
    best_performing_industry = "Ceramics & Granite"

# calculating number of lows each industry got
# Assign 0 to variables initially
fg_nlows = 0
aa_nlows = 0
cg_nlows = 0

# now, increment if Confidance is Low in that particular Industry
c.execute("SELECT Industry FROM Ticker WHERE Confidance == 'Low'")
low_info = c.fetchall()

for line in low_info:
    if line[0] == "Finance - General":
        fg_nlows += 1
    elif line[0] == "Auto Ancillaries":
        aa_nlows += 1
    else:
        cg_nlows += 1


# Checking which Industry is Worst Listed Indusrty
# Based on the Max no. of Lows it got
if(fg_nlows >= aa_nlows and fg_nlows >= cg_nlows):
    worst_performing_industry = "Finance - General"
elif(aa_nlows >= fg_nlows and aa_nlows >= cg_nlows):
    worst_performing_industry = "Auto Ancillaries"
else:
    worst_performing_industry = "Ceramics & Granite"

# Stoing Best Listed Industry & Worst Listed Industry in lists
bli = ["Best Listed Industry", best_performing_industry]
wli = ["Worst Listed Industry", worst_performing_industry]

# Insert those Best Listed Industry & Worst Listed Industry into Metrics
c.execute("INSERT INTO Metrics VALUES (?, ?)", bli)
c.execute("INSERT INTO Metrics VALUES (?, ?)", wli)

# Now, selecting particular data to calculate gain/loss % of Company
# where date = 20-05-2022, first day
c.execute(
    """SELECT Company_Name, Current_Price, Previous_Day_Price
    FROM Ticker
    WHERE Date_ == '20-05-2022'
    """
)

# fetching that data to a list
list_1 = c.fetchall()

# Now, Selecting price of company on last day, i.e. 24-05-2022
c.execute("SELECT Current_Price FROM Ticker WHERE Date_ == '24-05-2022'")

# fetching that data to a list
list_2 = c.fetchall()

i = 0
for line in list_1:
    # calculating change in price %
    change_perc = ((list_2[i][0] - list_1[i][1]) * 100) / list_1[i][1]
    change_perc = round(change_perc, 10)
    # storing data in list
    store_l = [list_1[i][0], list_1[i][1], list_2[i][0], change_perc]
    # inserting data into Temp1 table
    c.execute("INSERT INTO Temp1 VALUES (?, ?, ?, ?)", store_l)
    # incrementing i
    i += 1

# getting maximum change in price from Temp1 table using MAX
c.execute(
    """SELECT *
    FROM Temp1
    WHERE Change_In_Price = (SELECT MAX(Change_In_Price)
                             FROM Temp1)
    """
)
# fetching that data into val
val = c.fetchall()

# inserting values into Temp2 table
flist = []
for line in val:
    flist.append([float(line[2]) - float(line[1]), line[0], line[3]])
for line in flist:
    c.execute("INSERT INTO Temp2 VALUES(?, ?, ?)", line)

# Now getting maximum change
c.execute(
    """SELECT Names, Change, Percent
    FROM Temp2
    WHERE Change = (SELECT MAX(Change) FROM Temp2)
    """
)

val = c.fetchall()
# if more companies having same maximum change
# sorting with alphabetical order
val.sort()

# inserting best company name into Metrics
best_comp = ["Best Company_Name", val[0][0]]
c.execute("INSERT INTO Metrics VALUES(?, ?)", best_comp)

# inserting Gain % into Metrics
store_gain = ["Gain %", val[0][2]]
c.execute("INSERT INTO Metrics VALUES(?, ?)", store_gain)

# and deleting this all from temp2 and reusing this for worst ones
c.execute("DELETE FROM Temp2")

# getting minimum change in price to calculte loss %
c.execute(
    """SELECT *
    FROM Temp1
    WHERE Change_In_Price = (SELECT MIN(Change_In_Price)
                             FROM Temp1)
    """
)

val = c.fetchall()

flist = []
for line in val:
    flist.append([float(line[2]) - float(line[1]), line[0], line[3]])

# inserting data into Temp2 table
for line in flist:
    c.execute("INSERT INTO Temp2 VALUES(?, ?, ?)", line)

# Now getting minimum change
c.execute(
    """SELECT Names, Change, Percent
    FROM Temp2
    WHERE Change = (SELECT MIN(Change) FROM Temp2)
    """
)
val = c.fetchall()
# if more companies having same maximum change
# sorting with alphabetical order (from 'z' to 'a')
val.sort()

# inserting worst company name into metrics
# -1 representing that picking albhabet from last first
worst_comp = ["Worst Company_Name", val[-1][0]]
c.execute("INSERT INTO Metrics VALUES(?, ?)", worst_comp)

# inserting Loss % into Metrics
store_loss = ["Loss %", -val[-1][2]]
c.execute("INSERT INTO Metrics VALUES(?, ?)", store_loss)

# Dropping those 2 temporary tables we used
c.execute("DROP TABLE Temp2")
c.execute("DROP TABLE Temp1")

# committing
op.commit()

# closing
op.close()
