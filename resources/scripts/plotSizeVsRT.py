import numpy
from matplotlib import pyplot as plt

data=numpy.genfromtxt('test.csv', skiprows=1, delimiter=',')

timestamps = data[:,0]
latency = data[:,1]
size = data[:,8]
size[:] = [(s/1024.0/1024.0) for s in size]

plt.plot(size, latency, '*')
plt.title('RT vs Size')
plt.xlabel('size [MB]')
plt.ylabel('response time [ms]')
plt.ticklabel_format(style='plain')

plt.savefig('testPlot.png',dpi=100)

