from zipfile import ZipFile
import os
import pandas as pd
  
# specifying the zip file name
file_name = "US_CMPFile.zip"
  
# opening the zip file in READ mode
with ZipFile(file_name, 'r') as zip:
    # extracting all the files
    print('Extracting all the files now...')
    ab = zip.namelist()[0]
    zip.extractall()
    
    os.rename(ab,"US_CMPFile.csv")


    