from dataflows import Flow

from ..core import BaseDataGenusProcessor
from .load import LoaderDGP
from .transform import TransformDGP
from .enrich import EnricherDGP


class SimpleDGP(BaseDataGenusProcessor):

    def init(self,
             steps = [
                 LoaderDGP,
                 TransformDGP,
                 EnricherDGP,
             ]):

        self.steps = self.init_classes(steps)

    def flow(self):
        flows = []
        for i, step in enumerate(self.steps):
            flow = step.flow()
            if flow:
                flows.append(flow)
            else:
                break
            flow = step.preflow()
            if flow:
                flows.insert(0, flow)
        return Flow(*flows)
