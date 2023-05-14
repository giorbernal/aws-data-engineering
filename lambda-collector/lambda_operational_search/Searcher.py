class Searcher:

    def __init__(self, client=None):
        if client is None:
            # TODO implement as Connector for collector
            self.client = None
        else:
            self.client = client
        pass

    def get_data(self, main_search_key):
        # TODO
        pass
