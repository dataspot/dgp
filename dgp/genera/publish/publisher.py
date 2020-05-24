from ...core import ConfigurableDGP


class PublisherDGP(ConfigurableDGP):

    def init(self):
        super().init('publishing')
