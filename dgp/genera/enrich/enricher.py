from ...core import BaseDataGenusProcessor


class EnricherDGP(BaseDataGenusProcessor):

    def init(self):
        self._flows = None

    def analyze(self):
        if not self.steps:
            analyzers = self.context.taxonomy.analyzers(self.config, self.context)
            if analyzers is not None:
                self.steps = self.init_classes(
                    self.context.taxonomy.analyzers
                )
        super().analyze()

    @property
    def flows(self):
        if self._flows is None:
            self._flows = self.context.taxonomy.flows(self.config, self.context)
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
