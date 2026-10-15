import json
import matplotlib.pyplot as plt
import os

json_data = {
    "hour_2015": {"0": 469971, "1": 355145, "2": 268133, "3": 198524, "4": 143271, "5": 127437, "6": 268455, "7": 456127, "8": 561802, "9": 580034, "10": 567818, "11": 596504, "12": 637479, "13": 635587, "14": 658887, "15": 648688, "16": 576598, "17": 668790, "18": 799587, "19": 805230, "20": 733952, "21": 711579, "22": 686959, "23": 592429},
    "hour_2024": {"0": 1192044, "1": 775046, "2": 504187, "3": 331790, "4": 243817, "5": 264391, "6": 586403, "7": 1133880, "8": 1562647, "9": 1709199, "10": 1831576, "11": 1991265, "12": 2178786, "13": 2266970, "14": 2432603, "15": 2512081, "16": 2561465, "17": 2813259, "18": 2955735, "19": 2598187, "20": 2358283, "21": 2405757, "22": 2228352, "23": 1731941},
    "day_2024": {"1": 1299959, "2": 1315419, "3": 1324843, "4": 1296307, "5": 1361764, "6": 1398449, "7": 1338191, "8": 1354040, "9": 1404412, "10": 1408800, "11": 1384784, "12": 1402007, "13": 1388081, "14": 1451759, "15": 1395637, "16": 1419930, "17": 1420814, "18": 1420119, "19": 1359186, "20": 1412906, "21": 1382449, "22": 1340464, "23": 1320211, "24": 1300799, "25": 1256599, "26": 1291825, "27": 1296339, "28": 1265784, "29": 1263323, "30": 1159015, "31": 735449},
    "hist_dist_2024": {"bins": [0.01, 9.2544, 18.4988, 27.7432, 36.9876, 46.232, 55.4764, 64.7208, 73.9652, 83.2096, 92.454], "counts": [36586888, 3008583, 730117, 48925, 10092, 3788, 1730, 822, 415, 228]},
    "hist_amt_2024": {"bins": [0.01, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0], "counts": [1022926, 17236328, 11683050, 4264036, 1635072, 850967, 832252, 758564, 742960, 890412]},
    "top_10_zones_2024": {"JFK Airport": 1989986, "Upper East Side South": 1915438, "Midtown Center": 1914605, "Upper East Side North": 1729895, "Midtown East": 1418846, "Times Sq/Theatre District": 1391157, "Penn Station/Madison Sq West": 1362156, "Lincoln Square East": 1314690, "LaGuardia Airport": 1295499, "Murray Hill": 1176580}
}

output_dir = 'eda_plots'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

hours = [str(i) for i in range(24)]
h2015 = [json_data['hour_2015'].get(h, 0) for h in hours]
h2024 = [json_data['hour_2024'].get(h, 0) for h in hours]

plt.figure(figsize=(10, 6))
plt.plot(hours, h2015, label='2015', marker='o')
plt.plot(hours, h2024, label='2024', marker='s')
plt.title('Hourly Taxi Demand: 2015 vs 2024')
plt.xlabel('Hour of Day')
plt.ylabel('Trips')
plt.legend()
plt.grid(True, alpha=0.3)
plt.savefig(f'{output_dir}/hourly_demand.png')
plt.close()

zones = json_data['top_10_zones_2024']
sorted_zones = dict(sorted(zones.items(), key=lambda item: item[1]))
plt.figure(figsize=(12, 7))
plt.barh(list(sorted_zones.keys()), list(sorted_zones.values()), color='skyblue')
plt.title('Top 10 Pickup Zones (2024)')
plt.tight_layout()
plt.savefig(f'{output_dir}/top_zones.png')
plt.close()

plt.figure(figsize=(10, 6))
plt.bar(json_data['hist_dist_2024']['bins'][:10], json_data['hist_dist_2024']['counts'], width=5, color='salmon', edgecolor='black')
plt.title('Trip Distance Distribution (2024)')
plt.xlabel('Miles')
plt.savefig(f'{output_dir}/distance_dist.png')
plt.close()

plt.figure(figsize=(10, 6))
plt.bar(json_data['hist_amt_2024']['bins'][:10], json_data['hist_amt_2024']['counts'], width=8, color='gold', edgecolor='black')
plt.title('Fare Amount Distribution (2024)')
plt.xlabel('Total Amount ($)')
plt.savefig(f'{output_dir}/fare_dist.png')
plt.close()

days = sorted([int(d) for d in json_data['day_2024'].keys()])
counts = [json_data['day_2024'][str(d)] for d in days]
plt.figure(figsize=(12, 6))
plt.bar(days, counts, color='lightgreen')
plt.title('Daily Trips (January 2024)')
plt.xticks(days)
plt.savefig(f'{output_dir}/daily_trips.png')
plt.close()
