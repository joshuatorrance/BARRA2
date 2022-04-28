
# IMPORTS
from pandas import read_csv
from matplotlib import pyplot as plt


# PARAMETERS
csv_path = "/g/data/hd50/jt4085/BARRA2/jma_wind/data/hemispheres.csv"


# SCRIPT
def main():
    data = read_csv(csv_path, parse_dates=["Datetime"])
    data['IsNorth'] = data['Hemisphere']=='n'
    data['IsSouth'] = data['Hemisphere']=='s'

    mtsat_1r = data[data['SatelliteName']=='MTSAT-1R']
    mtsat_2 = data[data['SatelliteName']=='MTSAT-2']

    plt.figure()

    plt.plot(mtsat_1r['Datetime'], mtsat_1r['IsNorth'],
             'x', label="MTSAT-1R - Northern")
    plt.plot(mtsat_1r['Datetime'], -mtsat_1r['IsSouth'],
             'x', label="MTSAT-1R - Southern")
    plt.plot(mtsat_2['Datetime'], mtsat_2['IsNorth'],
             '+', label="MTSAT-2 - Northern")
    plt.plot(mtsat_2['Datetime'], -mtsat_2['IsSouth'],
             '+', label="MTSAT-2 - Southern")

    plt.xlabel("Datetime")
    plt.ylabel("Hemisphere Present")


    plt.figure()

    plt.bar(mtsat_1r['Datetime'], mtsat_1r['IsNorth'], label="MTSAT-1R North",
            bottom=[1]*len(mtsat_1r))
    plt.bar(mtsat_1r['Datetime'], mtsat_1r['IsSouth'], label="MTSAT-1R South",
            bottom=[0]*len(mtsat_1r))
    plt.bar(mtsat_2['Datetime'], mtsat_2['IsNorth'], label="MTSAT-2 North",
            bottom=[3]*len(mtsat_2))
    plt.bar(mtsat_2['Datetime'], mtsat_2['IsSouth'], label="MTSAT-2 South",
            bottom=[2]*len(mtsat_2))

    plt.yticks([])
    
    plt.xlabel("Datetime")
    plt.ylabel("Hemisphere Present")

    plt.legend()

    plt.show()

if __name__=="__main__":
    main()

