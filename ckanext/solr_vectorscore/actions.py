from ckan.logic.action.get import package_search as _package_search
from ckan.lib.search import PackageSearchQuery
import ckan.lib.search as search
import logging
import ckan.plugins.toolkit as toolkit
import functools 
import ckan.model as model
import requests
import re
from ckanext.solr_vectorscore.search import custom_query_for
from ckan.lib.search import query_for

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())
embedding_service_base_url = 'http://localhost:9090/embedding/'

def get_sbert_embeddings(text_input):
    query_encoded = requests.get(embedding_service_base_url, params={'sentence': text_input})
    embedding = query_encoded.json()['embedding']
    embeddings_string = ','.join(map(str, embedding))
    return embeddings_string 

@toolkit.side_effect_free
def package_search(context, data_dict):
    # Override the search_for function for packages in ckan.lib.search 
    # Attention: needs to be tested before taking this to production
    algorithm = sbert(data_dict)
    if algorithm:
        data_dict['fq'] = re.sub('algorithm:"'+ algorithm + '"', "", data_dict['fq'])
    # log.debug(data_dict)
    # log.debug(data_dict['fq'])
    if 'q' in data_dict and ':' not in data_dict['q'] and data_dict['q']and algorithm == "sbert":
        log.debug("The sbert algorithm is used!")
        search.query_for = functools.partial(custom_query_for)
        # log.debug('User search: querying with vector score')
        """ Override to translate title and description of the dataset. """
        embeddings = get_sbert_embeddings(data_dict['q'])
        data_dict['q'] = '{!vp f=vector vector="'+ embeddings +'" cosine=false}'
        data = _package_search(context, data_dict)

        return data
    else:
        search.query_for = query_for
        log.debug('No user search')
        return _package_search(context, data_dict)


def sbert(data_dict):
    if 'fq' in data_dict and data_dict['fq']:
        algorithm = re.search('sbert|bm25', data_dict['fq'])
        if algorithm:
            return algorithm.group()
    else:
        return None