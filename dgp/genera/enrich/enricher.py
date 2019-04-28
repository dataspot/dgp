from ...core import BaseDataGenusProcessor


class EnricherDGP(BaseDataGenusProcessor):

    def init(self):
        self._flows = None

    @property
    def flows(self):
        if self._flows is None:
            self._flows = self.context.taxonomy.flows(self.config, self.context)
        return self._flows

    def preflow(self):
        return self.flows[0]

    def flow(self):
        from dataflows import Flow, printer
        return Flow(
            self.flows[1],
            printer()
        )
