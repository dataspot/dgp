from ...core import ConfigurableDGP


class EnricherDGP(ConfigurableDGP):

    def init(self):
        super().__init__('processing')
        self._flows = None
