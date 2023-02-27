# -*- coding: future_fstrings -*-

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import logging
import requests
import base64
from ckan.logic.action.get import package_search
from ckanext.solr_vectorscore import actions
import os

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

embedding_service_base_url = 'http://localhost:9090/embedding/'

class SolrVectorscorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IActions)
    # plugins.implements(plugins.IFacets)


    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic',
            'solr_vectorscore')

    """
    # IRoutes
    def before_map(self, map):
        map.connect('/vector_search',
                    controller='ckanext.solr_vectorscore.controller.custom_vector_search:SolrVectorscoreController', 
                    action='package_search')
        return map
    """

    # IActions
    
    def get_actions(self):
        return {
            'package_search': actions.package_search
        }
    
    def to_solr_vector(self, vectors):
        """
        Source: https://github.com/DmitryKey/bert-solr-search/tree/master/src

        Takes BERT vectors array and converts into indexed representation: like so:
        1|point_1 2|point_2 ... n|point_n
        :param vectors: BERT vector points
        :return: Solr-friendly indexed representation targeted for indexing
        """
        solr_vector = []
        for vector in vectors:
            for i, point in enumerate(vector):
                solr_vector.append(str(i) + '|' + str(point))
        solr_vector = " ".join(solr_vector)
        return solr_vector

    # IPackageController
    def before_index(self, pkg_dict):
        """On each update, this function modifies what is getting passed to the SOLR index"""
        data_title = pkg_dict['title']
        data_abstract = pkg_dict['notes']
        data_string = "{0}: {1}".format(data_title, data_abstract)
        r = requests.get(embedding_service_base_url, params={'sentence': data_string})
        embedding =  self.to_solr_vector([r.json()['embedding']])
        pkg_dict['vector'] = embedding
        return pkg_dict

    
    def get_sbert_embeddings(self, text_input):
        query_encoded = requests.get(embedding_service_base_url, params={'sentence': text_input})
        embedding = query_encoded.json()['embedding']
        embeddings_string = ','.join(map(str, embedding))
        return embeddings 

    def before_search(self, search_params):
        return search_params

    """ def dataset_facets(self, facets_dict, package_type):
        # This keeps the existing facet order.
        facets_dict['algorithm'] = plugins.toolkit._('Algorithm') 

        # Return the updated facet dict.
        return facets_dict """

    """
    def package_search_test(self, context, data_dict):
        original_package_search = package_search
        results = original_package_search(context, data_dict)

        log.debug("TEST")
        log.debug("data: {}".format(data))

        return results
    """
    


    """
    def search(self):
        context = {'model': model, 'user': c.user, 'auth_user_obj': c.userobj}
        data_dict = {'q': '*:*', 'vq':''}
        log.debug("Im in custom search function :)")
        try:
            # Get the user query from the request parameters
            user_query = toolkit.request.params.get('q')
            vq = toolkit.request.params.get('vq')
            if vq:
                data_dict['vq'] = vq
                data_dict['q'] = vq + data_dict.get('q', '')
            
            # Get the embeddings from the SBERT service using the user_query
            embeddings = self.get_sbert_embeddings(user_query)
            # Encode the embeddings as a bytes object
            embeddings_bytes = bytes(embeddings)
            # Encode the bytes object in base64
            embeddings_base64 = base64.b64encode(embeddings_bytes)
            # Add the base64-encoded embeddings as a query parameter to the data_dict
            data_dict['embeddings'] = embeddings_base64
            
            result = toolkit.get_action('package_search')(context, data_dict)
        except toolkit.ValidationError as e:
            errors = e.error_dict
            error_summary = e.error_summary
            return self._render_template('search/search.html', extra_vars={'errors': errors, 'error_summary': error_summary})
        c.search_facets = result['search_facets']
        c.search_facets_limits = result.get('search_facets_limits', {})
        c.facets = result['facets']
        c.page = h.Page(
            collection=result['results'],
            page=result['page'],
            url=h.pager_url,
            item_count=result['count'],
            items_per_page=20
        )
        return self._render_template('search/search.html')
    """

    """ 
    def before_search(self, search_params):
    
        #{!vp f=vector vector="0.1,4.75,0.3,1.2,0.7,4.0" cosine=false}
       
        
        if 'q' in search_params and ':' not in search_params['q']:
            query_encoded = requests.get(embedding_service_base_url, params={'sentence': search_params['q']})
            embedding = query_encoded.json()['embedding']
            embeddings_string = ','.join(map(str, embedding))
            query_param_vector = '{!vp f=vector vector="'+ embeddings_string +'" cosine=false}'
            search_params['vq'] = query_param_vector
            search_params_mod = search_params
            log.debug('search_params_mod: {}'.format(search_params_mod))
            return search_params_mod

        else:
            return search_params

    """