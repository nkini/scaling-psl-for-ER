# coding: utf-8

# In[1]:

import pandas as pd
from itertools import combinations
from collections import defaultdict
from pyjarowinkler import distance as jwdistance
import string
import math
from sklearn.feature_extraction.text import TfidfVectorizer 

# Set the directory where the predicate files will be written here
outdir = 'data/cora-uwash/psl2-test-datagen'

df = pd.read_json('data/cora-uwash/cora_uwash.processed.json')

# You can select a subset of the dataset for which the predicates files will be created
#dfsubset = df[:2].append(df[3:5])
# dfsubset = df.sample(100)
# dfsubset = df.sample(200)
dfsubset = df[:]


print("Processing dataset with {} of the original {} records".format(len(dfsubset),len(df)))

print("Storing predicate files in {}".format(outdir))

# In[123]:

# So, now that  we have (possibly noisy) ground truth for authors, 
#    we can get to writing our blocking stats function.
def get_blocking_stats(blocks_dict, ground_truth_set, num_total_elements):
    '''
    Req: ground_truth_set is a set of tuples (el1,el2) s.t. el1 < el2
         blocks_dict      is a dict with block_key : set(el1, el2, ...)
    Modifies ground_truth_set.
    '''
    num_total_comparisons = num_total_elements * (num_total_elements - 1) / 2
    num_ground_truth_total = len(ground_truth_set)
    num_comparisons_after_blocking = 0
    ground_truth_set_unseen = ground_truth_set.copy()
    for block in blocks_dict.values():
        num_comparisons_after_blocking += len(block)*(len(block)-1)/2
        for el1,el2 in combinations(block,2):
            ground_truth_set_unseen.discard((el1, el2))
            ground_truth_set_unseen.discard((el2, el1))
    num_blocked_ground_truth = num_ground_truth_total - len(ground_truth_set_unseen)
    recall = num_blocked_ground_truth / num_ground_truth_total
    reduction_ratio = num_comparisons_after_blocking / num_total_comparisons
    return recall, reduction_ratio


# In[112]:

# SamePub truth and target
dfsubset['key'] = 1
dftruth_cross = dfsubset[['pubid','class_no','key']].merge(dfsubset[['pubid','class_no','key']], on='key')
dftruth_cross['same'] = (dftruth_cross['class_no_x'] == dftruth_cross['class_no_y']).astype(int)
dftruth_cross['zeros'] = 0
dftruth_cross[dftruth_cross['pubid_x'] != dftruth_cross['pubid_y']][['pubid_x','pubid_y','same']].to_csv(outdir+'/SamePub.truth.txt', sep='\t', header=False, index=False)
dftruth_cross[dftruth_cross['pubid_x'] != dftruth_cross['pubid_y']][['pubid_x','pubid_y','zeros']].to_csv(outdir+'/SamePub.target.txt', sep='\t', header=False, index=False)


print("Contains {} unique publications".format(len(dfsubset.class_no.unique())))

# In[119]:

authorlist = []
for index, row in dfsubset.iterrows():
    for mentionid, authname, authid in zip(row['author_mention_ids'],row['authors_as_list'],row['authors_ground_truth_id']):
        d = {}
        d['mentionid'], d['authname'], d['id'] = mentionid, authname, authid
        authorlist.append(d)
dfauthors = pd.DataFrame(authorlist)

print("Contains {} unique authors".format(len(dfauthors.id.unique())))

# In[120]:

# SameAuthor truth and target
dfauthors['key'] = 1
dftruth_auth_cross = dfauthors.merge(dfauthors, on='key')
dftruth_auth_cross['same'] = (dftruth_auth_cross['id_x'] == dftruth_auth_cross['id_y']).astype(int)
dftruth_auth_cross['zeros'] = 0
dftruth_auth_cross[dftruth_auth_cross['mentionid_x'] != dftruth_auth_cross['mentionid_y']][['mentionid_x','mentionid_y','same']].to_csv(outdir+'/SameAuthor.truth.txt', sep='\t', header=False, index=False)
dftruth_auth_cross[dftruth_auth_cross['mentionid_x'] != dftruth_auth_cross['mentionid_y']][['mentionid_x','mentionid_y','zeros']].to_csv(outdir+'/SameAuthor.target.txt', sep='\t', header=False, index=False)


# In[122]:

coauthorpairs = []
for index, row in dfsubset.iterrows():
    for auth1, auth2 in combinations(row['author_mention_ids'],2):
        coauthorpairs.append((auth1, auth2))
        coauthorpairs.append((auth2, auth1))
dfcoauthors = pd.DataFrame(coauthorpairs)
dfcoauthors.to_csv(outdir+'/AreCoAuthors.txt', sep='\t', header=False, index=False)


# In[124]:

# Calculating Ground Truth set
ground_truth = set()
for index, row in dftruth_auth_cross[dftruth_auth_cross['same'] == 1][['mentionid_x','mentionid_y']].iterrows():
    if row['mentionid_x'] < row['mentionid_y']:
        ground_truth.add((row['mentionid_x'],row['mentionid_y']))


# In[125]:
print("Computing blocks for Authors")

# Blocking condition of sub-super string relationship
dfauthors['bc'] = dfauthors['authname'].apply(lambda x : ''.join(sorted([part_of_name[0] for part_of_name in x.replace('.','. ').split() if (part_of_name !='' and part_of_name !='.')])))

blockkeys = set()
for key in  dfauthors['bc'].unique():
    blockkeys.add(key)
        
blocks3 = defaultdict(set)
for blockkey in blockkeys:
    for index, row in dfauthors.iterrows():
        if row['bc'] in blockkey:
            blocks3[blockkey].add(row['mentionid'])
            
recall, compression = get_blocking_stats(blocks3, ground_truth, len(dfauthors))
print("Number of blocks:",len(blocks3))
print("Recall: {:.4f}, Compression: {:.4f}".format(recall, compression))


# In[133]:

# Create the blocks dataframe and file for authors
blockid = 1
blocks_list = []
for key, mentionset in blocks3.items():
    for mentionid in mentionset:
        blocks_list.append((mentionid, blockid))
    blockid += 1
dfauthors_blocks = pd.DataFrame(blocks_list,columns=['mentionid','blockid'])
dfauthors_blocks.to_csv(outdir+'/BlocksAuthors.txt', sep='\t', header=False, index=False)


# In[135]:

# Create blocks file for publications (based on year)
dfsubset[['pubid','year_processed']].to_csv(outdir+'/BlocksPubs.txt', sep='\t', header=False, index=False)


# In[171]:

# Calculate the similarities of author names and save it in a HaveSimilarNames predicate file
# Here's a challenge: do it only within blocks
dfauthors_with_block_info = dfauthors[['mentionid','authname']].merge(dfauthors_blocks, on='mentionid')
auth_blocked_cross = dfauthors_with_block_info.merge(dfauthors_with_block_info, on='blockid')
view = auth_blocked_cross.query('mentionid_x < mentionid_y').drop_duplicates(subset=['mentionid_x','mentionid_y'])

with open(outdir+'/HaveSimilarNames.txt','w') as f:
    for index, row in view.iterrows():
        dist = jwdistance.get_jaro_distance(row['authname_x'],row['authname_y'], winkler=True)
        f.write('{}\t{}\t{}\n'.format(row['mentionid_x'],row['mentionid_y'],dist))
        f.write('{}\t{}\t{}\n'.format(row['mentionid_y'],row['mentionid_x'],dist))


# In[179]:

# Similarly, create a Publication blocks dataframe, keep the author and title fields
# Then write the HaveSimilarAuthors and HaveSimilarTitles predicate files
dfpubs_blocked_cross = dfsubset[['pubid','year_processed','title','author']].merge(dfsubset[['pubid','year_processed','title','author']], on='year_processed')
view_pub = dfpubs_blocked_cross.query('pubid_x < pubid_y')

with open(outdir+'/HaveSimilarAuthors.txt','w') as fSimAuth:
    for index, row in view_pub.iterrows():
        distAuth = jwdistance.get_jaro_distance(row['author_x'],row['author_y'], winkler=True)
        fSimAuth.write('{}\t{}\t{}\n'.format(row['pubid_x'],row['pubid_y'],distAuth))
        fSimAuth.write('{}\t{}\t{}\n'.format(row['pubid_y'],row['pubid_x'],distAuth))


# TF-IDF based similarity calculations for publication title
all_documents = []
pubids_list = []
for index, row in df.iterrows():
    all_documents.append(row['title'])
    pubids_list.append(row['pubid'])

blockpubs = set()
for index, row in view_pub.iterrows():
    blockpubs.add((row['pubid_x'],row['pubid_y']))

def cosine_similarity(vector1, vector2):
    dot_product = sum(p*q for p,q in zip(vector1, vector2))
    magnitude = math.sqrt(sum([val**2 for val in vector1])) * math.sqrt(sum([val**2 for val in vector2]))
    if not magnitude:
        return 0
    return dot_product/magnitude

tokenize = lambda doc: doc.lower().split(" ")
sklearn_tfidf = TfidfVectorizer(norm='l2',min_df=0, use_idf=True, smooth_idf=False, sublinear_tf=True, tokenizer=tokenize)   
sklearn_representation = sklearn_tfidf.fit_transform(all_documents)

with open(outdir+'/HaveSimilarTitles.txt','w') as f: 
    for pubid_0, title_0 in zip(pubids_list, sklearn_representation.toarray()):
        for pubid_1, title_1 in zip(pubids_list, sklearn_representation.toarray()):
            if pubid_0 < pubid_1 and (pubid_0, pubid_1) in blockpubs:
                sim = cosine_similarity(title_0, title_1)
                f.write('{}\t{}\t{:.4f}\n'.format(pubid_0,pubid_1,sim))
                f.write('{}\t{}\t{:.4f}\n'.format(pubid_1,pubid_0,sim))


# In[180]:

# HasAuthor for each publication
with open(outdir+'/HasAuthor.txt','w') as f:
    for index, row in dfsubset.iterrows():
        for mentionid in row['author_mention_ids']:
            f.write('{}\t{}\n'.format(row['pubid'],mentionid))
