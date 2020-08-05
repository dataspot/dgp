from .....core import BaseAnalyzer, Validator, Required
from .....config.consts import CONFIG_HEADER_FIELDS, CONFIG_TAXONOMY_ID, CONFIG_MODEL_EXTRA_FIELDS, \
        CONFIG_TAXONOMY_CT, CONFIG_CONSTANTS, CONFIG_MODEL_MAPPING
from .....config.log import logger


class MappingGuesserAnalyzer(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_HEADER_FIELDS),
        Required(CONFIG_TAXONOMY_ID),
        Required(CONFIG_TAXONOMY_CT),
        Required(CONFIG_CONSTANTS),
        Required(CONFIG_MODEL_MAPPING),
    )

    def get_mapping(self, fields, known, existing, extraFields, cts):
        mapping = []
        for field in fields:
            if field is not None:
                if field in existing:
                    mapping.append(existing[field])
                else:
                    for kf, kv in known.items():
                        if field.lower() == kf.lower():
                            ct = kv.get('type')
                            full_ct = cts.get(ct)
                            if full_ct is None:
                                logger.info('Failed to find columnType entry for known mapping %s -> %s', kf, kv)
                                continue
                            normalize = kv.get('normalize')
                            if normalize is not None:
                                normalizeTarget, normalize = normalize.get('header'), normalize.get('using')
                            else:
                                normalizeTarget = None
                            if ct is None:
                                field_mapping = dict(
                                    name=field,
                                    title=field,
                                    normalize=normalize,
                                    normalizeTarget=normalizeTarget
                                )
                                extraFields.update(
                                    ('extra', k)
                                    for k in normalize.keys()
                                )
                                extraFields.add(('normalize', normalizeTarget))
                            else:
                                field_mapping = dict(
                                    name=field,
                                    title=full_ct.get('title') or field,
                                    description=full_ct.get('description') or None,
                                    options=full_ct.get('options') or {},
                                    columnType=ct
                                )
                            mapping.append(field_mapping)
                            break
                    else:
                        field_mapping = dict(
                            name=field,
                            title=field,
                            columnType=None
                        )
                        mapping.append(field_mapping)

        return mapping

    def run(self):
        fields = self.config[CONFIG_HEADER_FIELDS]
        # types = self.config[CONFIG_TAXONOMY_CT]
        taxonomy_id = self.config[CONFIG_TAXONOMY_ID]
        constants = dict(self.config[CONFIG_CONSTANTS])
        current_mapping = self.config[CONFIG_MODEL_MAPPING]

        known = self.context.taxonomies.get(taxonomy_id).header_mapping
        cts = self.context.taxonomies.get(taxonomy_id).column_types
        for ct in cts:
            for f in ('title', 'name'):
                if ct[f] not in known:
                    known[ct[f]] = dict(type=ct['name'])
        cts = dict(
            (ct['name'], ct)
            for ct in cts
        )
        if len(current_mapping) > 0:
            # Avoid re-guessing mapping after first round
            known = {}
        existing = dict(
            (entry.get('name'), entry)
            for entry in current_mapping
            if any(entry.get(k) is not None for k in ('columnType', 'normalize'))
        )

        extraFields = set(
            ('constant', k, v)
            for k, v
            in constants.items()
        )
        extraFields.update(set(
            ('extra', k)
            for f in current_mapping
            for k in f.get('normalize', {}).keys()
        ))
        extraFields.update(set(
            ('normalize', f['normalizeTarget'])
            for f in current_mapping
            if 'normalizeTarget' in f
        ))

        mapping = []
        mapping.extend(self.get_mapping(fields, known, existing, extraFields, cts))
        extraFields = list(map(list, sorted(extraFields)))
        extraFieldsMapping = self.get_mapping([x[1] for x in extraFields],
                                              known, existing, None, cts)
        assert len(extraFields) == len(extraFieldsMapping)
        mapping.extend(extraFieldsMapping)

        self.config[CONFIG_MODEL_MAPPING] = mapping
        self.config[CONFIG_MODEL_EXTRA_FIELDS] = extraFields
