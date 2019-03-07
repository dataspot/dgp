from dataflows import Flow

from ..core import BaseDataGenusProcessor
from .load import LoaderDGP
from .transform import TransformDGP
from .enrich import EnricherDGP


class SimpleDGP(BaseDataGenusProcessor):

    def init(self,
             post_load_flow=None,
             post_transform_flow=None):

        self.steps = self.init_classes([
            LoaderDGP,
            TransformDGP,
            EnricherDGP,
        ])
        self.post_flows = [
            post_load_flow,
            post_transform_flow,
            None
        ]

    def flow(self):
        flows = []
        for i in range(len(self.steps)):
            flow = self.steps[i].flow()
            if flow:
                flows.append(flow)
                flow = self.post_flows[i]
                if flow:
                    flows.append(flow)
            else:
                break
        return Flow(*flows)
