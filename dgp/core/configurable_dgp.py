from .base_dgp import BaseDataGenusProcessor


class ConfigurableDGP(BaseDataGenusProcessor):

    def init(self, kind):
        self._flows = None
        self._kind = kind

    def analyze(self):
        if not self.steps:
            if self.context.taxonomy:
                analyzers = self.module.analyzers(self.config, self.context)
                if analyzers is not None:
                    self.steps = self.init_classes(analyzers)
        return super().analyze()

    @property
    def module(self):
        return getattr(self.context.taxonomy, self._kind)

    @property
    def flows(self):
        if self._flows is None:
            self._flows = self.module.flows(self.config, self.context)
        return self._flows

    def preflow(self):
        if self.flows:
            return self.flows[0]

    def flow(self):
        from dataflows import Flow
        if self.flows:
            return Flow(
                self.flows[1],
            )
        else:
            return Flow(
            )
