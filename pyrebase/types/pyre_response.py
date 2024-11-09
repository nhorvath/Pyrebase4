from collections import OrderedDict

class PyreResponse:
    def __init__(self, pyres, query_key):
        self.pyres = pyres
        self.query_key = query_key

    def __getitem__(self,index):
       return self.pyres[index]

    def val(self):
        if isinstance(self.pyres, list) and self.pyres:
            # unpack pyres into OrderedDict
            pyre_list = []
            # if firebase response was a list
            if isinstance(self.pyres[0].key(), int):
                for pyre in self.pyres:
                    pyre_list.append(pyre.val())
                return pyre_list
            # if firebase response was a dict with keys
            for pyre in self.pyres:
                pyre_list.append((pyre.key(), pyre.val()))
            return OrderedDict(pyre_list)
        else:
            # return primitive or simple query results
            return self.pyres

    def key(self):
        return self.query_key

    def each(self):
        if isinstance(self.pyres, list):
            return self.pyres