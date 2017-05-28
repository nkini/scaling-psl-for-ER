import json
import numpy as np
from collections import defaultdict
from itertools import combinations


def get_num_blocked_pairs_analytical2(blocks, dataset1_name, dataset2_name):
    num_blocked_pairs = 0
    alloverlaps = set()
    blocks = list(blocks.values())
    for block1Index in range(len(blocks)):

        #print('Processing block {}'.format(block1Index))

        block1_d1 = blocks[block1Index][dataset1_name]
        block1_d2 = blocks[block1Index][dataset2_name]

        a = len(block1_d1) * (len(block1_d2)) / 2
        b = 0
        c = 0
        overlaps_for_this_block = set()
        for block2Index in range(len(blocks)):
            if block1Index == block2Index:
                continue

            block2_d1 = blocks[block2Index][dataset1_name]
            block2_d2 = blocks[block2Index][dataset2_name]

            #overlaps = list(combinations((block1_d1 & block2_d1) & (block1_d2 & block2_d2), 2))
            overlaps = set()
            for r1 in block1_d1 & block2_d1:
                for r2 in block1_d2 & block2_d2: 
                    overlaps.add((r1, r2))

            b += len(overlaps)
            for overlap in overlaps:
                if overlap in overlaps_for_this_block:
                    c += 1

            overlaps_for_this_block.update(overlaps)
            alloverlaps.update(overlaps)

        num_blocked_pairs += a - b + c
        #print("len(alloverlaps) so far:", len(alloverlaps))

    num_blocked_pairs += len(alloverlaps)
    print('final number of pairs after blocking (calculated analytically, method 2):', num_blocked_pairs)
    return num_blocked_pairs


def get_num_blocked_pairs_straightfwd(blocks, dataset1_name, dataset2_name):
    num_blocked_pairs = 0
    for key in blocks:
        ds1len = len(blocks[key][dataset1_name])
        ds2len = len(blocks[key][dataset2_name])
        num_blocked_pairs += ds1len * ds2len / 2
    
    print('final number of pairs after blocking (calculated straightforward:', num_blocked_pairs)
    return num_blocked_pairs   


def standard_blocking_stats2(dataset1, dataset2, blocking_key, answer_key, dataset1_name, dataset2_name):
    '''
    dataset1: list [ dict ({
                        id: ...,
                        feature1: ...,
                        feature2: ..., 
                        ...
                        }),
                     dict ({
                        id: ...,
                        feature1: ...,
                        feature2: ..., 
                        ...
                        }),
                     ...
                    ]
    dataset2: list [ dict ({
                        id: ...,
                        feature1: ...,
                        feature2: ..., 
                        ...
                        }),
                     dict ({
                        id: ...,
                        feature1: ...,
                        feature2: ..., 
                        ...
                        }),
                     ...
                    ]
    blocking_key: 'featureR'
    answer_key: list[ 
                    dict({dataset1_name:<id>,dataset2_name:<id>} ),
                    dict({dataset1_name:<id>,dataset2_name:<id>} ),
                ]
    '''
    ent_ref_map = defaultdict(set)
    print("Willy wonky!")
    
    # create blocks
    blocks = defaultdict(lambda:defaultdict(set))
    for record in dataset1:
        blocks[record[blocking_key]][dataset1_name].add(record['id'])
    for record in dataset2:
        blocks[record[blocking_key]][dataset2_name].add(record['id'])
    '''
    with open('years.txt','w') as f:
        for key in blocks.keys():
            f.write(str(key)+'\n')
    '''
    print('number of elements in block 0: dataset1: {}, dataset2: {}'.format(len(blocks[0][dataset1_name]),len(blocks[0][dataset2_name])))
    print('created blocks. number of blocks:', len(blocks))
            
    # create ground truth
    groundtruth_d1_elems, groundtruth_d2_elems = zip(*[(answer_pair[dataset1_name],answer_pair[dataset2_name]) for answer_pair in answer_key])
    ent_ref_ground_truth = set(zip(groundtruth_d1_elems, groundtruth_d2_elems))
    print('created ground truth. number of pairs:',len(ent_ref_ground_truth))
    print('number of pairs in block 0 that are in the ground truth: dataset1: {}, dataset2: {}'.format(len(blocks[0][dataset1_name] & set(groundtruth_d1_elems)), len(blocks[0][dataset2_name] & set(groundtruth_d2_elems))))
            
    print('\nBlocking on %s\n' % blocking_key)
    
    # calculate num pairs to compare post blocking
    #num_pairs_after_blocking1 = get_num_blocked_pairs_explicit(blocks)
    #num_pairs_after_blocking2 = get_num_blocked_pairs_analytical2(blocks, dataset1_name, dataset2_name)
    num_pairs_after_blocking2 = get_num_blocked_pairs_straightfwd(blocks, dataset1_name, dataset2_name)
    #assert num_pairs_after_blocking1 == num_pairs_after_blocking2

    num_pairs_recalled = 0
    for refid1,refid2 in ent_ref_ground_truth:
        for blockid, block in blocks.items():
            if refid1 in block[dataset1_name] and refid2 in block[dataset2_name]:
                num_pairs_recalled += 1
                break

    # Calculate reduction ratio
    dataset1num = len(dataset1)
    dataset2num = len(dataset2)
    num_comp_before_blocking = dataset1num*dataset2num/2
    num_comp_after_blocking = num_pairs_after_blocking2
    print('Number of elements in dataset 1:',dataset1num)
    print('Number of elements in dataset 2:',dataset2num)
    print('Num comparisons without blocking: %d' % num_comp_before_blocking)
    print('Num comparisons after blocking: %d' % num_comp_after_blocking)
    reduction_ratio = (1 - num_comp_after_blocking/num_comp_before_blocking)
    print('Reduction ratio: %f' % reduction_ratio)
    
    # Calculate recall
    recall = num_pairs_recalled/len(ent_ref_ground_truth)
    print('Recall: %f' % recall)
    print('\n')
    return recall, reduction_ratio
    


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
    #num_pairs_after_blocking2 = get_num_blocked_pairs_analytical2(blocks)
    num_pairs_after_blocking2 = get_num_blocked_pairs_straightfwd(blocks, dataset1_name, dataset2_name)
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
    # b1: {r1, r3, r4}, b3: {r3, r4}, b5: {r1, r2}, b6: {r1, r2}, b7: {r3, r4}
    assert round(recall, 4) == round(1/2 , 4)
    assert round(reduction_ratio, 4) == round(1/3 , 4)
    # b12: {r1, r2, r3, r4}, b45: {r1, r2, r3, r4}, b65: {r1, r2, r3, r4},
    recall, reduction_ratio = test_standard_blocking('ipaddress')
    assert round(recall, 4) == round(1.0000 , 4)
    assert round(reduction_ratio, 4) == round(0.0000 , 4)
    # b1: {r1, r3}, b2: {r2, r4},
    recall, reduction_ratio = test_standard_blocking('ua')
    assert round(recall, 4) == round(1.0000 , 4)
    assert round(reduction_ratio, 4) == round(2/3 , 4)
