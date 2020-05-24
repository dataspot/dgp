from ...core import ConfigurableDGP


class EnricherDGP(ConfigurableDGP):

    def init(self):
        super().init('processing')
        self._flows = None
