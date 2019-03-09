from dataflows import Flow

from ..core import BaseDataGenusProcessor
from .load import LoaderDGP
from .transform import TransformDGP
from .enrich import EnricherDGP


class SimpleDGP(BaseDataGenusProcessor):

    def init(self,
             post_load_flow=None,
             post_transform_flow=None,
             post_enrich_flow=None,
             publish_flow=None):

        self.steps = self.init_classes([
            LoaderDGP,
            TransformDGP,
            EnricherDGP,
        ])
        self.post_flows = [
            post_load_flow,
            post_transform_flow,
            post_enrich_flow,
        ]
        self.publish_flow = publish_flow

    def flow(self):
        flows = []
        for i, step in enumerate(self.steps):
            flow = step.flow()
            if flow:
                flows.append(flow)
                flow = self.post_flows[i]
                if flow:
                    flows.append(flow)
                flow = step.preflow()
                if flow:
                    flows.insert(0, flow)
            else:
                return Flow(*flows)
        if self.publish_flow:
            flows.append(self.publish_flow)
        return Flow(*flows)
