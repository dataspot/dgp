from dataflows import Flow

from ..core import BaseDataGenusProcessor
from ..config.log import logger
from .load import LoaderDGP, PostLoaderDGP
from .transform import TransformDGP
from .enrich import EnricherDGP


class SimpleDGP(BaseDataGenusProcessor):

    def init(self,
             steps=[
                 LoaderDGP,
                 PostLoaderDGP,
                 TransformDGP,
                 EnricherDGP,
             ]):

        self.steps = self.init_classes(steps)

    def flow(self):
        flows = []
        for i, step in enumerate(self.steps):
            logger.debug('Adding step %s to the flow', step.__class__.__name__)
            flow = step.flow()
            if flow:
                flows.append(flow)
            else:
                logger.debug('Step %s caused flow building to stop', step.__class__.__name__)
                break
            flow = step.preflow()
            if flow:
                flows.insert(0, flow)
        return Flow(*flows)
