import json
import numpy as np
from collections import defaultdict
from itertools import combinations
from pybloom import ScalableBloomFilter


def get_num_blocked_pairs_explicit(blocks):
    num_pairs_after_blocking = 0
    itnum = 0

    # False positive matches are possible, but false negatives are not – in other words, a query returns either "possibly in set" or "definitely not in set". 
    sbf = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
    for blockid, block in blocks.items():
        itnum += 1
        #print('processing block {} out of {} ({} references)'.format(itnum, len(blocks), len(block)))
        for pair in combinations(block,2):
            if pair not in sbf:
                sbf.add(pair)
                num_pairs_after_blocking += 1
        #print('num_pairs_after_blocking:',num_pairs_after_blocking)
    print('final number of pairs after blocking (calculated by explicitly evaluating the pairs):', num_pairs_after_blocking)
    return num_pairs_after_blocking


def get_num_blocked_pairs_analytical(blocks):
    num_blocked_combinations = 0
    for i in range(len(blocks)):
        print('processing sets of size {}'.format(i+1))
        for combo in combinations(blocks.values(), i+1):
            l = len(set.intersection(*combo))
            if i%2 == 0:
                num_blocked_combinations += l*(l-1)/2
            else:
                num_blocked_combinations -= l*(l-1)/2
    print('final number of pairs after blocking (calculated analytically):', num_blocked_combinations)
    return num_blocked_combinations


def standard_blocking_stats(data, blocking_key):
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
    #num_pairs_after_blocking1 = get_num_blocked_pairs_explicit(blocks)
    num_pairs_after_blocking2 = get_num_blocked_pairs_analytical(blocks)
    #assert num_pairs_after_blocking1 == num_pairs_after_blocking2

    num_pairs_recalled = 0
    for refid1,refid2 in ent_ref_ground_truth:
        for blockid, block in blocks.items():
            if refid1 in block and refid2 in block:
                num_pairs_recalled += 1
                break

    # Calculate reduction ratio
    num_comp_before_blocking = (num_references*(num_references - 1))/2
    num_comp_after_blocking = num_pairs_after_blocking2
    print('Num comparisons without blocking: %d' % num_comp_before_blocking)
    print('Num comparisons after blocking: %d' % num_comp_after_blocking)
    reduction_ratio = (1 - num_comp_after_blocking/num_comp_before_blocking)
    print('Reduction ratio: %f' % reduction_ratio)
    
    # Calculate recall
    recall = num_pairs_recalled/len(ent_ref_ground_truth)
    print('Recall: %f' % recall)
    print('\n')
    return recall, reduction_ratio

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
     
    return standard_blocking_stats(data, blocking_key)

if __name__  == '__main__':    
    recall, reduction_ratio = test_standard_blocking('region')
    assert round(recall, 4) == round(1/2 , 4)
    assert round(reduction_ratio, 4) == round(1/3 , 4)
    recall, reduction_ratio = test_standard_blocking('ipaddress')
    assert round(recall, 4) == round(1.0000 , 4)
    assert round(reduction_ratio, 4) == round(0.0000 , 4)
    recall, reduction_ratio = test_standard_blocking('ua')
    assert round(recall, 4) == round(1.0000 , 4)
    assert round(reduction_ratio, 4) == round(2/3 , 4)
