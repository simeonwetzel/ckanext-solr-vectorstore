import ckan.model as model
import ckan.plugins.toolkit as toolkit
import ckan.plugins as plugins


class SolrVectorscoreController(toolkit.BaseController):
    plugins.implements(plugins.IPackageController, inherit=True)

    def package_search(self, context, data_dict):
        """
        Custom search function for CKAN packages
        """
        log.debug("Using custom package_search() function")
        log.debug(data_dict)
        return results
