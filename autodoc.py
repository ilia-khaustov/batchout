from glob import glob

from jinja2 import Environment, FileSystemLoader

from batchout.core.registry import Registry


def render_docs_components():
    jinja2_env = Environment(loader=FileSystemLoader('templates'), autoescape=False)
    template = jinja2_env.get_template('components.md.j2')
    for base_cls, components_by_type in Registry.BOUND.items():
        md_path = next(iter(glob(f'docs/components/*_{base_cls.PLURAL_ALIAS}.md')), None)
        if md_path is None:
            continue
        spec_by_type = {}
        for component_type, component_cls in components_by_type.items():
            full_spec = {}
            for spec_cls in getattr(component_cls, 'spec.*', []):
                full_spec |= getattr(component_cls, f'spec.{spec_cls}', {})
            spec_by_type[component_type] = list(sorted(
                [(key, props) for key, props in full_spec.items()],
                key=lambda p: (not p[1]['required'], p[0]),
            ))
        md_content = template.render(
            base_cls=base_cls,
            components_by_type=components_by_type,
            spec_by_type=spec_by_type,
        )
        with open(md_path, mode='w') as f:
            f.write(md_content)


if __name__ == "__main__":
    render_docs_components()
