import matplotlib.pyplot as plt
import numpy as np
# us_dict = dict()
# ca_dict = dict()
# 3: canada, 24: us
us_month = []
us_shipments = []
ca_month = []
ca_shipments = []

with open('q5.txt','r') as f:
    line = f.readline()
    while line:
        nation,month,shipments = line.split(',')
        nation = nation[1:]
        shipments = shipments.split(')')[0]
        # print(nation,month,shipments)
        if nation == "24":
            # us_dict[month] = int(shipments)
            us_month.append(month)
            us_shipments.append(int(shipments))
        elif nation == "3":
            # ca_dict[month] = int(shipments)
            ca_month.append(month)
            ca_shipments.append(int(shipments))
        line = f.readline()


fig = plt.figure(figsize=(8,6))
plt.plot(us_month,us_shipments, label=("US"), linewidth=2.0, color ="blue")
plt.plot(ca_month,ca_shipments, label=("CA"), linewidth=2.0, color ="red")

plt.title('Monthly Shipments, US vs CA ',fontsize=20)
plt.legend(prop={'size': 10},loc="upper right")
plt.xlabel("Month", fontsize=15)
plt.ylabel("# of shipments", fontsize=15)
# plt.grid(linestyle='dashed')
plt.xticks(us_month, fontsize=5, rotation = 90)
plt.yticks(fontsize=5)
plt.savefig('Q5.jpg')
plt.show()

