#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


lgbtq = pd.read_csv("yuanting_mingyuli_si618_hw4_batch_result_LGBTQ.csv")
lgbtq_subset1 = lgbtq[['Input.tweet_text','Answer.lgbtq1.supports-lgbtq-movements',
                'Answer.lgbtq2.opposes-some-lgbtq-movements','Answer.lgbtq3.neutral/unclear']]
tweets = lgbtq['Input.tweet_text'].nunique()
tweets


# In[3]:


supports = lgbtq.groupby(['Input.tweet_text'])[['Answer.lgbtq1.supports-lgbtq-movements']].sum()
opposes = lgbtq.groupby(['Input.tweet_text'])[['Answer.lgbtq2.opposes-some-lgbtq-movements']].sum()
neutral = lgbtq.groupby(['Input.tweet_text'])[['Answer.lgbtq3.neutral/unclear']].sum()


# In[4]:


with open("yuanting_mingyuli_si618_hw4_computations_lgbtq.txt",'w') as f:
    # at least 1
    f.write("supports-lgbtq-movements1\t"+str(supports[supports['Answer.lgbtq1.supports-lgbtq-movements']>=1].shape[0]/tweets))
    f.write("\t opposes-some-lgbtq-movements1\t"+
            str(opposes[opposes['Answer.lgbtq2.opposes-some-lgbtq-movements']>=1].shape[0]/tweets))
    f.write("\t neutral-lgbtq1 \t"+str(neutral[neutral['Answer.lgbtq3.neutral/unclear']>=1].shape[0]/tweets)+"\n")
    # at least 2
    f.write("supports-lgbtq-movements2\t"+
            str(supports[supports['Answer.lgbtq1.supports-lgbtq-movements']>=2].shape[0]/tweets))
    f.write("\t opposes-some-lgbtq-movements2\t"+
            str(opposes[opposes['Answer.lgbtq2.opposes-some-lgbtq-movements']>=2].shape[0]/tweets))
    f.write("\t neutral-lgbtq2\t"+str(neutral[neutral['Answer.lgbtq3.neutral/unclear']>=2].shape[0]/tweets)+"\n")
    # at least 3
    f.write("supports-lgbtq-movements3\t"+
            str(supports[supports['Answer.lgbtq1.supports-lgbtq-movements']==3].shape[0]/tweets))
    f.write("\t opposes-some-lgbtq-movements3\t"+
            str(opposes[opposes['Answer.lgbtq2.opposes-some-lgbtq-movements']==3].shape[0]/tweets))
    f.write("\t neutral-lgbtq3\t"+str(neutral[neutral['Answer.lgbtq3.neutral/unclear']==3].shape[0]/tweets))
    f.write("\n")


# In[5]:


lgbtq_subset2 = lgbtq[['Input.tweet_text','Input.created_time','Answer.lgbtq1.supports-lgbtq-movements',
                      'Answer.lgbtq2.opposes-some-lgbtq-movements','Answer.lgbtq3.neutral/unclear']]
month = '-'.join(lgbtq_subset2['Input.created_time'][0].split('-')[0:2])
lgbtq_subset2['month'] = pd.Series(lgbtq_subset2.loc[:,('Input.created_time')].str.split('-').str[0:2])
lgbtq_subset2['month'] = lgbtq_subset2['month'].str.join('-')


# In[6]:


sum2support = lgbtq_subset2.groupby(['Input.tweet_text'])


# In[7]:


supporttf = sum2support.filter(lambda x: x['Answer.lgbtq1.supports-lgbtq-movements'].sum()>=2)
monthsupport = supporttf.groupby('month')[['Input.tweet_text']].nunique()


# In[8]:


opposetf = sum2support.filter(lambda x: x['Answer.lgbtq2.opposes-some-lgbtq-movements'].sum()>=2)
monthoppose = opposetf.groupby(['month'])[['Input.tweet_text']].nunique()


# In[9]:


neutraltf = sum2support.filter(lambda x: x['Answer.lgbtq3.neutral/unclear'].sum()>=2)
monthneutral = neutraltf.groupby(['month'])[['Input.tweet_text']].nunique()


# In[10]:


nomajority = sum2support.filter(lambda x: (x['Answer.lgbtq3.neutral/unclear'].sum()<2)&
                                (x['Answer.lgbtq2.opposes-some-lgbtq-movements'].sum()<2)&
                               (x['Answer.lgbtq1.supports-lgbtq-movements'].sum()<2))
monthmajority = nomajority.groupby(['month'])[['Input.tweet_text']].nunique()


# In[11]:


months_count = lgbtq_subset2.groupby(['month'])[['Input.tweet_text']].nunique()


# In[12]:


with open("yuanting_mingyuli_si618_hw4_computations_lgbtq.txt",'a') as f:
    f.write("\n")
    for i in range(len(monthsupport)):
        f.write(monthsupport.index[i].split('-')[0]+monthsupport.index[i].split('-')[1]+"\n") # write year+month
        f.write("supports-lgbtq-movements-majority\t"+
                str(monthsupport['Input.tweet_text'][i]/months_count['Input.tweet_text'][i]))
        f.write("\topposes-some-lgbtq-movements-majority\t"+
                str(monthoppose['Input.tweet_text'][i]/months_count['Input.tweet_text'][i]))
        f.write("\tneutral majority\t"+
                str(monthneutral['Input.tweet_text'][i]/months_count['Input.tweet_text'][i]))
        f.write("\tno_majority\t"+
                str(monthmajority['Input.tweet_text'][i]/months_count['Input.tweet_text'][i]))
        f.write("\n")


# In[13]:


lgbtq_subset3 = lgbtq[['Input.tweet_text','Answer.lgbtq1.supports-lgbtq-movements',
                       'Answer.lgbtq2.opposes-some-lgbtq-movements','Answer.lgbtq3.neutral/unclear',
                      'Answer.targeted1.yes']]


# In[14]:


targetgroup = lgbtq_subset3.groupby(['Input.tweet_text'])


# In[15]:


sup_t = targetgroup.filter(lambda x: (x['Answer.targeted1.yes'].sum()>=1) &
                                 (x['Answer.lgbtq1.supports-lgbtq-movements'].sum()>=2))
num_sup = targetgroup.filter(lambda x: x['Answer.lgbtq1.supports-lgbtq-movements'].sum()>=2)['Input.tweet_text'].nunique()


# In[16]:


ops_t = targetgroup.filter(lambda x: (x['Answer.targeted1.yes'].sum()>=1) &
                                 (x['Answer.lgbtq2.opposes-some-lgbtq-movements'].sum()>=2))
num_ops = targetgroup.filter(lambda x: x['Answer.lgbtq2.opposes-some-lgbtq-movements'].sum()>=2)['Input.tweet_text'].nunique()


# In[17]:


neu_t = targetgroup.filter(lambda x: (x['Answer.targeted1.yes'].sum()>=1) &
                                 (x['Answer.lgbtq3.neutral/unclear'].sum()>=2))
num_neu = targetgroup.filter(lambda x: x['Answer.lgbtq3.neutral/unclear'].sum()>=2)['Input.tweet_text'].nunique()


# In[18]:


with open("yuanting_mingyuli_si618_hw4_computations_lgbtq.txt",'a') as f:
    f.write("\n")
    f.write("targeted_supports-lgbtq-movements-majority\t"+str(sup_t['Input.tweet_text'].nunique()/num_sup))
    f.write("\ttargeted_opposes-some-lgbtq-movements-majority\t"+str(ops_t['Input.tweet_text'].nunique()/num_ops))
    f.write("\ttargeted_neutral-majority\t"+str(neu_t['Input.tweet_text'].nunique()/num_neu))

