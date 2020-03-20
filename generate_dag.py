import os
import yaml
from jinja2 import Environment, FileSystemLoader
from typing import List


def _generate_dag(yaml_load: List) -> None:
    """Generating a dag file from an airflow template
    : param: python object (converted from yaml.load())
    :return: an output file named as {dag_name} which saved under output folder
    """
    template_loader = FileSystemLoader(airflow_template_dir_path)
    template_env = Environment(loader=template_loader)

    for info in yaml_load:
        template_name = info.get("jinja2_template")
        template = template_env.get_template(template_name)
        dag_name = info.get("dag_name")
        outfile = os.path.join(output_dir_path, dag_name + ".py")
        output_dag = template.render(info)
        print("generating the output dag file...")

        with open(outfile, "w", encoding="utf-8") as out_file:
            print("writing the output dag file to {OUTPATH}...".format(OUTPATH=outfile))
            out_file.write(output_dag)
        out_file.close()


def main():
    """Reading in yaml file(s) from the config folder
    """
    for filename in os.listdir(config_dir_path):
        config_file_path = os.path.join(config_dir_path, filename)
        with open(config_file_path) as in_file:
            _generate_dag(yaml.load(in_file))


if __name__ == "__main__":
    # define input and output directories
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir_path = os.path.join(base_dir, "config")
    output_dir_path = os.path.join(base_dir, "output")
    airflow_template_dir_path = os.path.join(base_dir, "airflow_template")

    main()
