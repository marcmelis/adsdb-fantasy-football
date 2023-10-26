#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import glob
from datetime import date


# In[2]:


os.chdir('../data/landing/temporal')


# In[3]:


for filename in glob.glob('*.*'):
  print(f"Moving {filename}")
  filebase, fileextension = filename.split(".")
  os.rename(os.path.join(os.getcwd(), filename),
            os.path.join(os.getcwd(), '../persistent/', f'{filebase}_{date.today()}.{fileextension}'))


# In[ ]:




