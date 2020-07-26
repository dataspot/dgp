from .base_dgp import BaseDataGenusProcessor


class ConfigurableDGP(BaseDataGenusProcessor):

    def init(self, kind, per_taxonomy=True):
        self._flows = None
        self._analyzers = None
        self._kind = kind
        self._per_taxonomy = per_taxonomy

    def analyze(self):
        if self.analyzers is not None:
            return super().analyze()
        return True

    @property
    def module(self):
        if self._per_taxonomy:
            ret = getattr(self.context.taxonomy, self._kind)
            if ret and ret.has_module():
                return ret
        return getattr(self.context.taxonomies.get('_common_'), self._kind)

    @property
    def flows(self):
        if self.module is not None and self._flows is None:
            self._flows = self.module.flows(self.config, self.context)
        return self._flows

    @property
    def analyzers(self):
        if self.module is not None and self._analyzers is None:
            self._analyzers = self.module.analyzers(self.config, self.context) or []
            self.steps = self.init_classes(self._analyzers)
        return self._analyzers

    def preflow(self):
        if self.flows:
            return self.flows[0]

    def flow(self):
        from dataflows import Flow
        if self.flows:
            return Flow(
                self.flows[1],
            )
        elif self.analyzers:
            return super().flow()
        else:
            return Flow(
            )
