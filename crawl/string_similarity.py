# -*- coding: utf-8 -*-

import jellyfish
from difflib import SequenceMatcher
from pprint import pprint

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()
    # return jellyfish.levenshtein_distance(unicode(a), unicode(b))

def list_similarity(input, options, tops = 10):
	scores = []
	indices = [i for i in range(len(options))]
	
	for option in options:
		scores.append(similar(input, option))

	# [x for (y,x) in sorted(zip(Y,X))]
	return sorted(zip(scores, indices), reverse=True)[:tops]

if __name__ == "__main__":
	options = ["gato", "cachorro", "sapato", "macaco", "animal", "batata", "paralelepipedo", "baleia", "rato", "barata"]
	pprint([(options[y], x) for (x, y) in list_similarity("gato", options, 10)])