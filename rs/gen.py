from pyfinite import ffield
import random

n = 100
x, y, z = [], [], []

f = ffield.FField(8, gen=0b100011011, useLUT=0)

for i in range(n):
  x.append(random.randint(0, 255))
  y.append(random.randint(0, 255))
  z.append(f.Multiply(x[-1], y[-1]))

file = open('test.in', 'w')
file.writelines(['{} {} {}\n'.format(x[i], y[i], z[i]) for i in range(n)])
file.close()