import numpy
from matplotlib import pyplot as plt

data=numpy.genfromtxt('test.csv',skiprows=1,delimiter=',')

timestamps = data[:,0]
latency = data[:,1]
size = data[:,8]

#bins = numpy.linspace(-10, 10, 100)

#plt.plot(size,latency,'*',linewidth=0)
#plt.hist(size, bins, alpha=0.5)
plt.hist(size, 100, alpha=0.5)
plt.title('RT vs Size')
plt.xlabel('size [Byte]')
plt.ylabel('db')

plt.savefig('testPlot.png',dpi=100)

