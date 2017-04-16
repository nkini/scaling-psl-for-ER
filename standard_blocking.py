import json
import numpy as np
from collections import defaultdict
from itertools import combinations

def standard_blocking(data, blocking_key):
    '''
    data: dict ({
                <reference_id>: dict ({
                                'features': dict ({...}),
                                'entity_id': ...
                            })
            })
            
    blocking_key: key such that data[<reference_id>]['features'][<blocking_key>]
            returns an iterable with values to block on.
    '''
    num_references = len(data)
    ent_ref_map = defaultdict(set)
    
    # create blocks and ground truth
    blocks = defaultdict(set)
    for refid, refdata in data.items():
        ent_ref_map[refdata['entity_id']].add(int(refid))
        for it in data[refid]['features'][blocking_key]:
            blocks[it].add(int(refid))
    print('created blocks. Number of blocks:',len(blocks))
            
    ent_ref_ground_truth = set()
    for entid, refs in ent_ref_map.items():
        for pair in combinations(refs, 2):
            ent_ref_ground_truth.add(pair)
    print('created ground truth. number of pairs:',len(ent_ref_ground_truth))
            
    print('\nBlocking on %s\n' % blocking_key)
    
    # calculate num pairs to compare post blocking
    num_pairs_after_blocking = []
    pairs_after_blocking = set()
    num_pairs_recalled = 0
    itnum = 0

    for blockid, block in blocks.items():
        itnum += 1
        print('processing block {} out of {} ({} references)'.format(itnum, len(blocks), len(block)))
        #num_pairs_after_blocking.append(len(block)*(len(block)-1)/2)
        pairs_after_blocking.update([pair for pair in combinations(block,2)])
        print('len(pairs_after_blocking):',len(pairs_after_blocking))
    #print('upper bound on calculated number of pairs after blocking (because it contained repeats):', sum(num_pairs_after_blocking))
    print('number of pairs after blocking:', len(pairs_after_blocking))


    for refid1,refid2 in ent_ref_ground_truth:
        for blockid, block in blocks.items():
            if refid1 in block and refid2 in block:
                num_pairs_recalled += 1
                break

    # Calculate reduction ratio
    num_comp_before_blocking = (num_references*(num_references - 1))/2
    num_comp_after_blocking = len(pairs_after_blocking)
    print('Num comparisons without blocking: %d' % num_comp_before_blocking)
    print('Num comparisons after blocking: %d' % num_comp_after_blocking)
    #print('Num comparisons after blocking (upper bound): %d' % num_comp_after_blocking)
    print('Reduction ratio: %f' % (1 - num_comp_after_blocking/num_comp_before_blocking))
    #print('Reduction ratio (lower bound): %f' % (1 - num_comp_after_blocking/num_comp_before_blocking))
    
    # Calculate recall
    recall = num_pairs_recalled/len(ent_ref_ground_truth)
    print('Recall: %f' % recall)
    print('\n')

def test_standard_blocking(blocking_key):
    '''
    data: dict ({
                <reference_id>: dict ({
                                'features': dict ({...}),
                                'entity_id': ...
                            })
            })
    '''
    data = {
        '1' : {
            'features' : { 'region' : set([1, 5, 6]), 'ipaddress': set([12, 45, 65]), 'ua': set([1])},
            'entity_id': 2381374681
        },
        '2' : {
            'features' : { 'region' : set([5, 6]), 'ipaddress': set([12, 45, 65]), 'ua': set([2])},
            'entity_id': 2381374682
        },
        '3' : {
            'features' : { 'region' : set([1, 3, 7]), 'ipaddress': set([12, 45, 65]), 'ua': set([1])},
            'entity_id': 2381374681
        },
        '4' : {
            'features' : { 'region' : set([1, 3, 7]), 'ipaddress': set([12, 45, 65]), 'ua': set([2])},
            'entity_id': 2381374682
        }
    }
     
    standard_blocking(data, blocking_key)

if __name__  == '__main__':    
    test_standard_blocking('region')
    test_standard_blocking('ipaddress')
    test_standard_blocking('ua')
