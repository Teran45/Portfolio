import pandas as pd
import glob
import os
import sys


for file in glob.iglob('C:\\Users\\teran\\Desktop\\sber\\gp\\project\\**.xlsx'):
    read_file = pd.read_excel(file)
    read_file.to_csv(sys.stdout.buffer, index=None, header=True)


